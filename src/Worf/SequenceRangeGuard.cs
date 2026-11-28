using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
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

    // How many values of each gated sequence a row of the statements that hold its gate may draw: its default's, and the
    // calls of the CHECK constraints, trigger conditions and trigger functions of the tables they write, by the tables,
    // or as many as a row was seen to draw, as a trigger's loop or a function it calls draws more than its source shows.
    // The position each row of a gated statement was checked at, and the most values a row drew from each sequence in
    // the session's last statement that held its gate, up to as many as a block holds past its last twentieth, which
    // its next such statement starts from.
    private static readonly Dictionary<uint, long> s_gatedRowDraws = [];

    // The gated sequences whose rows may draw more than the statement's text shows, as a trigger's loop does, whose draws
    // the guard learns from the values the rows drew.
    private static readonly HashSet<uint> s_unseenRowDraws = [];
    private static readonly Dictionary<uint, long> s_rowMarks = [];
    private static readonly Dictionary<uint, long> s_seenRowDraws = [];
    private static readonly HashSet<uint> s_seenThisStatement = [];
    private static readonly Dictionary<string, (int Calls, bool Unseen)> s_writtenCalls = [];
    private static readonly Dictionary<uint, decimal> s_steps = [];
    private static readonly List<(nint Statement, uint Subxact, List<uint> Sequences)> s_gating = [];
    private static readonly Dictionary<uint, List<uint>> s_drawnBy = [];
    private static readonly Dictionary<uint, bool> s_afterConditionsDraw = [];

    // The managed sequences the triggers of each table and its partitions name among their arguments, by the table and
    // the events the triggers fire on.
    private static readonly Dictionary<(uint Relid, int Events, int MovedInto, bool Replica), List<uint>> s_arguedBy = [];

    // The trigger functions of a table's triggers, and its partitions', that fire on some events in this session's
    // replication role and name managed sequences among their arguments, with those sequences.
    private static readonly Dictionary<(uint Relid, int Events, int MovedInto, bool Replica), Dictionary<uint, List<uint>>> s_arguedFunctions = [];

    // The trigger functions of the statements running that the fmgr hook watches, innermost last, with the sequences
    // their triggers name for each, so that each call checks them.
    private static readonly List<(nint Statement, uint Subxact, Dictionary<uint, HashSet<uint>> Functions)> s_hookingStatements = [];
    private static bool s_hookingRegistered;

    // TRIGGER_TYPE_INSERT and TRIGGER_TYPE_UPDATE, the bits of pg_trigger.tgtype for the events a trigger fires on.
    private const int TriggerTypeInsert = 1 << 2;

    // TRIGGEROID, the type a trigger function returns.
    private const uint TriggerTypeOid = 2279;
    private const uint NextvalOid = 1574;

    // The type regclass, of the constant a call of nextval names its sequence with.
    private const uint RegclassOid = 2205;
    private const uint BigintOid = 20;
    private const uint DomainInOid = 2597;
    private const uint DomainReceiveOid = 2598;
    private const uint ArrayInOid = 750;
    private const uint ArrayReceiveOid = 2400;
    private const uint RecordInOid = 2290;
    private const uint RecordReceiveOid = 2402;
    private const uint RangeInOid = 3834;
    private const uint RangeReceiveOid = 3836;
    private const uint MultirangeInOid = 4231;
    private const uint MultirangeReceiveOid = 4233;
    private const int MaxReadDepth = 16;
    private const int MaxColumns = 1664;
    private const uint OctetLengthOid = 1374;
    private const uint CurrentSettingOid = 2077;
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
    // transaction it started in before and the transaction and subtransaction it starts in now, with the sequences its
    // calls name once a call has looked, and how many have.
    private static (nint Expr, (uint Started, uint Transaction, uint Subtransaction) Start, (HashSet<uint> Called, int Calls, bool Unnamed)? Draws, int Looked) s_simple;

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

    // The calls of nextval statements' plans make, kept as the sequences they name are.
    private static readonly Dictionary<(string Text, string Path, uint User), (HashSet<uint> Called, int Calls, bool Unnamed)> s_drawsByText = [];

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
    // TYPEOID, CONSTROID, PROCOID, NAMESPACEOID, SUBSCRIPTIONRELMAP and SEQRELID, the ids of the catalog caches of
    // types, constraints, functions, schemas, subscriptions' tables and sequences, which only PostgreSQL 19's
    // bindings name.
    private const int TypeCacheId = 80;
    private const int ConstraintCacheId = 19;
    private const int ProcedureCacheId = 45;
    private const int NamespaceCacheId = 36;
    private const int SubscriptionRelCacheId = 66;
    private const int SequenceCacheId = 59;
#elif ANKUS_PG18
    private const int TypeCacheId = 82;
    private const int ConstraintCacheId = 19;
    private const int ProcedureCacheId = 47;
    private const int NamespaceCacheId = 38;
    private const int SubscriptionRelCacheId = 68;
    private const int SequenceCacheId = 61;
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

    // Whether the transaction has taken on the callbacks that forget, as it ends, what the guard notes of its statements.
    private static bool s_endRegistered;

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
    /// Gets the shared counts of the draws under way that hold no gate, which a switch waits for.
    /// </summary>
    internal static PgShared<WorfFastDraws> FastDraws { get; } = new("worf_sequence_range_fast_draws");

    /// <summary>
    /// Sets up the shared memory.
    /// </summary>
    internal static void ShmemInit()
    {
        PgSharedMemory.Initialize(Drawers, MaxDrawers, static () => default, static _ => default);
        PgSharedMemory.Initialize(FastDraws);
    }

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

    // The neighbors a message of whose this server took in whole since it started, as the blocks were last read: one
    // this node exchanges with that has no report of a sequence, as tables restored from before it first reported have
    // none, is waited for, as a start's draws wait, until such a message shows whether it reports the sequence.
    private static long[] s_complete = [];

    // Whether this server's start was kept in the data directory's file of starts, as once seen it stays.
    private static bool s_startKept;

    // Whether this backend warned that it draws though the file of starts doesn't keep this server's start, and the
    // sequences, with the nodes they waited for, that it draws from though those nodes haven't answered since the start.
    private static bool s_startUnkeptWarned;
    private static readonly HashSet<(uint Seqoid, uint Node)> s_unanswered = [];

    // Whether the guard is reading the blocks to find the grantors a statement's plan names.
    private static bool s_namingGrantors;

    private static long s_localVersion = -1;

    // The furthest position each sequence is known to have reached, which only moves forward within a block.
    private static readonly Dictionary<uint, long> s_reached = [];

    // The spans each sequence may still issue, for a session that restores rows, as of the start of the statement they
    // were read for.
    private static readonly Dictionary<uint, List<(long Lo, long Hi)>> s_issuable = [];
    private static long s_issuableAt;

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

    // Counts the times the draws left, or those promised, were set anew or forgotten, rather than counted down: a
    // call that took some of them ahead, to draw without a count each time, then gives up what it has left.
    private static long s_allowanceEpoch;

    // The draws this backend's statements may still take from each sequence without checking it, as the shared table
    // counts them, and how many of those they took since it last counted.
    private static readonly Dictionary<uint, (long Promised, long Drawn)> s_promised = [];

    // The sequences whose gates this backend holds for a unit of a statement that draws without gates of its own, a row
    // of its plan or a PL/pgSQL statement, which it lets go as the unit ends, rather than for a statement that writes.
    private static readonly HashSet<uint> s_unitGates = [];

    // The executor runs, plan rows and PL/pgSQL statements under way that may hold such gates, innermost last: each with
    // its kind, what it is, the function state a PL/pgSQL statement runs under, the subtransaction a PL/pgSQL statement
    // started in, and where the gates it took start among those the scopes took, innermost last too.
    private static readonly List<(byte Kind, nint Key, nint Owner, uint Subxact, int First)> s_scopes = [];
    private static readonly List<uint> s_scopeGates = [];
    private const byte RunScope = 1;
    private const byte RowScope = 2;
    private const byte StatementScope = 3;

    // A PL/pgSQL statement whose expression starts a call of nextval, which holds the call's gates from then until
    // PL/pgSQL next tells of a statement of its function, by when the call has drawn.
    private const byte DrawScope = 4;

    // Whether PL/pgSQL's plugin is worf's own, which alone tells worf of its statements; and how many statement and
    // draw scopes are open, while any of which PL/pgSQL tells of its statements' ends.
    private static bool s_ownsPlugin;
    private static int s_statementScopes;

    // The statement run, as a QueryDesc, under which each sequence's count of draws before its next check was given, or
    // zero outside any, which gives it up as the run ends.
    private static readonly Dictionary<uint, nint> s_shareRuns = [];

    // worf's own PL/pgSQL plugin, or null where another library's was there first.
    private static unsafe PLpgSQL_plugin* s_plugin;

    // Whether PL/pgSQL tells worf of each statement it starts, and of each one's end, as WatchStatements has it.
    private static bool s_watchingStarts;
    private static bool s_watchingEnds;

    // The PL/pgSQL expressions that started a call of nextval since PL/pgSQL last told of a statement, each with the
    // subtransaction it started in, the state of the function running it, and whether that is a DO block's, whose
    // expressions go with it: an expression's state is there to change only once it has started.
    private static readonly List<(nint Expr, uint Subxact, nint Estate, bool Inline)> s_startedSites = [];

    // The calls of nextval in expressions' states that go through the fmgr hook this transaction, each with the managed
    // sequence it names, or zero where it names its sequence as it runs, its arguments and its function's lookup; where
    // each is among them, by its place in the expression's plan, the last started of those that share one, as two
    // runs of one plan do; and the transaction's number among those that had any, which a call keeps with its place
    // among them once it has looked it up.
    private static readonly List<(uint Named, nint Arguments, nint Function)> s_sites = [];
    private static readonly Dictionary<nint, int> s_siteOf = [];
    private static uint s_siteGeneration = 1;

    // The calls among them, by their call information, that a statement of one row makes, which counts no draws for
    // later rows, as it has none, so that it holds no values back from other sessions while it runs on.
    private static readonly HashSet<nint> s_singleCalls = [];

    // The places among them that the plans of statements still open took, each with its call's place in the plan and
    // its call information, by statement, as a QueryDesc, and those that statements that ended left free for the
    // next: a transaction may run any number of statements.
    private static readonly Dictionary<nint, List<(int Site, nint Call, nint Information)>> s_planSites = [];
    private static readonly Stack<int> s_freeSites = [];

    // Where PostgreSQL's nextval is, and GuardedNextval as PostgreSQL calls it; where PostgreSQL's function that wraps
    // a call in the fmgr hook is, or zero where calls of nextval don't go through it, and whether that was looked up;
    // and the sequence Snowflake node numbers come from, as a call started last.
    private static nint s_nextval;
    private static nint s_guardedNextval;
    private static nint s_hookedCall;
    private static bool s_hookedCallRead;
    private static uint s_snowflake;

    // What shared memory holds for the draws that hold no gate, which read it without a call into PostgreSQL; the
    // count there of those under way in this backend and those that share its place; how many changes to the blocks it
    // told of as this backend last read them; whether this backend has such a draw under way; and the call whose
    // function the fmgr hook replaced with GuardedNextval, until that call ends.
    private static unsafe long* s_fastDraws;
    private static unsafe long* s_fastSlot;
    private static long s_changesRead = -1;
    private static bool s_drawingFast;
    private static nint s_handedOff;

    // When this backend last looked in shared memory for a switch waiting for the gates it holds, in the stopwatch's
    // ticks, and how long a unit that held its gates already lets pass before it looks again: a tenth of a millisecond.
    // A switch so waits for the row or statement under way and no longer, and for those that take less, that long.
    private static long s_lookedAt;
    private static readonly long s_lookEvery = Math.Max(Stopwatch.Frequency / 10000, 1);
    private static bool s_countingDown;

    // Where each of those sequences was as it was last counted down for a call that names no sequence, which counts one
    // down only as far as it has moved since.
    private static readonly Dictionary<uint, long> s_unnamedMarks = [];

    // The plan nodes of the statements that draw so, with the functions that produce their rows, which the guard counts
    // down for before each row, by the statement, with the sequences the statement's calls name, or null when one names
    // none.
    private static readonly Dictionary<nint, (ExecProcNodeMtd Function, int Calls, IReadOnlyCollection<uint>? Named)> s_rowFunctions = [];
    private static readonly Dictionary<nint, (uint Subxact, List<nint> Nodes)> s_countedNodes = [];

    // The calls of nextval a statement has started as it ran, by statement, those of them found in the expression states
    // its tables keep, which go through the guard one by one, and how many had started as they were last looked for.
    // The plan nodes that count rows down only until those calls are found tell the statement they belong to.
    private static readonly Dictionary<nint, int> s_runStarted = [];
    private static readonly Dictionary<nint, (HashSet<nint> Found, int Looked)> s_runFound = [];
    private static readonly Dictionary<nint, nint> s_runRows = [];

    // The memory in which each VALUES list of a statement builds a row's expressions, with the list's node, by
    // statement. PostgreSQL builds them anew for each row and forgets them with it, so a call that starts there is that
    // row's alone, which its start checks, and no later row runs it again.
    private static readonly Dictionary<nint, (nint Memory, nint Node)[]> s_valuesRows = [];

    // The VALUES row whose calls last started, and how many of its calls have: a row starts all its calls before it
    // draws for any, so each start counts for those before it too, which the sequence doesn't show as drawn yet.
    private static (nint Node, int Row, int Started) s_valuesRow;

    // The running statements of which only the plan's top node counts its rows down, as VALUES rows that go to the
    // client have it do: each row the client reads lets go the gates its calls took as they started.
    private static readonly HashSet<nint> s_runTop = [];

    [PgNativeCallback(nameof(CountedRow))]
    private static partial ExecProcNodeMtd CountedRowCallback { get; }

    // The statements that draw from a sequence without its gate, as a SELECT of nextval does, which count among the
    // statements drawing from it from their first check on, until they end: by the depth of the statements starting as
    // each was counted, and the statement it is, once its plan has started.
    private static readonly List<(int Depth, nint Statement, uint Subxact, uint Seqoid)> s_ungatedDrawers = [];

    // The source plan nodes of the upserts and merges that hold gates, with the functions that produce their rows and
    // the sequences their rows draw from, which the guard checks before each row, and those sequences by the statement,
    // as it starts, until its plan has.
    private static readonly Dictionary<nint, (ExecProcNodeMtd Function, List<uint> Sequences)> s_gatedRows = [];
    private static readonly Dictionary<nint, List<uint>> s_rowsDrawing = [];

    [PgNativeCallback(nameof(GatedRow))]
    private static partial ExecProcNodeMtd GatedRowCallback { get; }

    [PgNativeCallback(nameof(PlpgsqlStatement))]
    private static partial PLpgSQL_plugin_stmt_begCallback PlpgsqlStatementCallback { get; }

    [PgNativeCallback(nameof(PlpgsqlStatementEnd))]
    private static partial PLpgSQL_plugin_stmt_endCallback PlpgsqlStatementEndCallback { get; }

    [PgNativeCallback(nameof(GuardedNextval))]
    private static partial PGFunction GuardedNextvalCallback { get; }

    // The points in each sequence's active block, by sequence and value, at which this backend has woken the range worker.
    private static readonly HashSet<(uint Seqoid, long Mark)> s_woken = [];

    // The active blocks, by sequence and start, whose switch this backend has waited for, until when it may, whether that
    // time ran out, and the start of the client statement that waited, as a later statement waits again.
    private static readonly Dictionary<(uint Seqoid, long Lo), (long Deadline, bool Expired, long Statement)> s_waited = [];

    // The neighbors this transaction dropped the subscriptions to, which its draws wait for still, as they would but for
    // the drop, until it commits: the drop may yet roll back, to a savepoint too, and the values drawn meanwhile with it.
    private static readonly Dictionary<uint, string> s_droppedNeighbors = [];
    private static bool s_droppedRegistered;

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
    /// The guard's trigger function, whose arguments list what the guard checks. A trigger that holds a condition, its
    /// own or the one a statement has it run, does its checking there, and the condition is never true, so this never
    /// runs for it. It runs for a trigger that holds none on a row no statement of this session's had it check or
    /// skip, as one PostgreSQL's apply of another subscription writes, and checks the row as the condition would.
    /// </summary>
    /// <param name="context">The trigger's context.</param>
    /// <returns>Nothing, as an AFTER trigger returns.</returns>
    [PgTrigger]
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_guard()
        RETURNS trigger LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static PgHeapTuple? Guard(PgTriggerContext context)
    {
        bool inserted = context.Operation == PgTriggerOperation.Insert;
        if (context.New is not PgHeapTuple row || (!inserted && context.Operation != PgTriggerOperation.Update) || WritesAsWorker())
        {
            return null;
        }

        foreach ((short column, bool numeric, uint[] sequences) in GuardedColumns(context.RelationOid, [.. context.Arguments]))
        {
            if (row[column - 1] is not object value)
            {
                continue;
            }

            if (numeric)
            {
                PgNumeric written = row.Get<PgNumeric>(column - 1);
                if (inserted || context.Old is not PgHeapTuple old || old[column - 1] is null || !Same(old.Get<PgNumeric>(column - 1), written))
                {
                    CheckUnder(sequences, written, inserted);
                }
            }
            else
            {
                long written = Convert.ToInt64(value, CultureInfo.InvariantCulture);
                if (inserted || context.Old is not PgHeapTuple old || old[column - 1] is not object before || Convert.ToInt64(before, CultureInfo.InvariantCulture) != written)
                {
                    CheckUnder(sequences, written, inserted);
                }
            }
        }

        return null;
    }

    // Whether this backend is one of worf's own apply or sync workers, which write other nodes' rows.
    private static bool WritesAsWorker() => Worker.MyWorfWorker >= 0 && Worker.MyWorker.WorkerType is WorfWorkerType.Apply or WorfWorkerType.Sync;

    // Checks a value under the sequences its column uses: a value that none of them admits is checked under each, so
    // that every one of them has its say.
    private static void CheckUnder(uint[] sequences, long value, bool inserted)
    {
        if (sequences.Length == 1 || !Array.Exists(sequences, seqoid => AdmitsReloaded(seqoid, value)))
        {
            foreach (uint seqoid in sequences)
            {
                _ = Check(seqoid, value, inserted);
            }
        }
    }

    private static void CheckUnder(uint[] sequences, PgNumeric value, bool inserted)
    {
        if (sequences.Length == 1 || !Array.Exists(sequences, seqoid => Whole(value) is decimal exact &&
            exact is >= long.MinValue and <= long.MaxValue && AdmitsReloaded(seqoid, (long)exact)))
        {
            foreach (uint seqoid in sequences)
            {
                _ = Check(seqoid, value, inserted);
            }
        }
    }

    /// <summary>
    /// Gets whether a function is the guard's trigger function, by what the guard has read of it alone, as the fmgr
    /// hook asks where it may call nothing of PostgreSQL's.
    /// </summary>
    /// <param name="fnOid">The function.</param>
    /// <returns>Whether it is.</returns>
    internal static bool IsGuardFunction(uint fnOid) => s_guardFunctions.TryGetValue(fnOid, out bool guard) && guard;

    /// <summary>
    /// Checks, as PostgreSQL looks a function up to call it, whether it is the guard's trigger function, which then
    /// runs through the fmgr hook: <see cref="GuardRow"/> checks the row in its place, with the row as PostgreSQL
    /// holds it. The function is told by its name and schema, read from the catalog cache, which a role without
    /// rights on worf's schema reads too.
    /// </summary>
    /// <param name="fnOid">The function.</param>
    /// <returns>Whether it is.</returns>
    internal static unsafe bool HooksGuard(uint fnOid)
    {
        if (!s_guardFunctions.TryGetValue(fnOid, out bool guard))
        {
            guard = false;
            if (NativeMethods.IsTransactionState() && NativeMethods.get_func_rettype(fnOid) == TriggerTypeOid)
            {
                sbyte* name = NativeMethods.get_func_name(fnOid);
                sbyte* schema = name is null ? null : NativeMethods.get_namespace_name(NativeMethods.get_func_namespace(fnOid));
                guard = schema is not null && MemoryMarshal.CreateReadOnlySpanFromNullTerminated((byte*)name).SequenceEqual("sequence_range_guard"u8) &&
                    MemoryMarshal.CreateReadOnlySpanFromNullTerminated((byte*)schema).SequenceEqual("worf"u8);
            }
            else if (!NativeMethods.IsTransactionState())
            {
                return false;
            }

            if (s_guardFunctions.Count >= 256)
            {
                s_guardFunctions.Clear();
            }

            s_guardFunctions[fnOid] = guard;
        }

        return guard;
    }

    /// <summary>
    /// Checks the row a guard trigger that holds no condition fires for, as the trigger's function, in its place: a
    /// new row's values, and those an update changes, under the sequences the trigger's arguments name for their
    /// columns. worf's own apply and sync workers write other nodes' rows, which it leaves alone.
    /// </summary>
    /// <param name="fcinfo">The call of the trigger's function.</param>
    internal static unsafe void GuardRow(FunctionCallInfoBaseData* fcinfo)
    {
        const uint EventOperation = 3;
        const uint EventUpdate = 2;
        const uint EventRow = 4;
        const uint Int2Oid = 21;
        const uint Int4Oid = 23;
        const uint NumericOut = 1702;
        TriggerData* fired = (TriggerData*)fcinfo->context;
        if (fired is null || fired->type != NodeTag.T_TriggerData || (fired->tg_event & EventRow) == 0 || WritesAsWorker())
        {
            return;
        }

        uint operation = fired->tg_event & EventOperation;
        bool inserted = operation == 0;
        HeapTupleData* row = inserted ? fired->tg_trigtuple : fired->tg_newtuple;
        HeapTupleData* old = inserted ? null : fired->tg_trigtuple;
        if ((!inserted && operation != EventUpdate) || row is null || fired->tg_relation is null || fired->tg_trigger is null)
        {
            return;
        }

        List<string> arguments = [];
        for (int i = 0; i < fired->tg_trigger->tgnargs; i++)
        {
            arguments.Add(Worf.FromServer(fired->tg_trigger->tgargs[i]));
        }

        TupleDescData* columns = fired->tg_relation->rd_att;
        foreach ((short column, bool numeric, uint[] sequences) in GuardedColumns(fired->tg_relation->rd_id, arguments))
        {
            bool isnull;
            ulong value = NativeMethods.heap_getattr(row, column, columns, &isnull);
            if (isnull)
            {
                continue;
            }

            bool wasNull = true;
            ulong was = old is null ? 0 : NativeMethods.heap_getattr(old, column, columns, &wasNull);
            if (numeric)
            {
                PgNumeric written = PgNumeric.Parse(Worf.FromServer(NativeMethods.OidOutputFunctionCall(NumericOut, value)));
                if (wasNull || !Same(PgNumeric.Parse(Worf.FromServer(NativeMethods.OidOutputFunctionCall(NumericOut, was))), written))
                {
                    CheckUnder(sequences, written, inserted);
                }
            }
            else
            {
                uint type = NativeMethods.TupleDescAttr(columns, column - 1)->atttypid;
                long Read(ulong datum) => type == Int2Oid ? (short)datum : type == Int4Oid ? (int)datum : (long)datum;
                if (wasNull || Read(was) != Read(value))
                {
                    CheckUnder(sequences, Read(value), inserted);
                }
            }
        }
    }

    /// <summary>
    /// Checks a value an update writes into a column that uses managed sequences, where it changes the column, and
    /// gives the value back: an INSERT's ON CONFLICT DO UPDATE has the value it sets such a column of a partitioned
    /// table to pass through here, as a partition the row reaches opens too late for its triggers to check it.
    /// </summary>
    /// <param name="sequences">The sequences the column uses, as their OIDs with commas between.</param>
    /// <param name="was">The value the row held.</param>
    /// <param name="value">The value the update sets.</param>
    /// <returns>The value the update sets; one outside this node's blocks fails.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_checked(sequences text, was bigint, value bigint)
        RETURNS bigint VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static long? CheckedValue(string? sequences, long? was, long? value)
    {
        if (sequences is not null && value is long written && was != written)
        {
            CheckUnder([.. sequences.Split(',').Select(seqoid => uint.Parse(seqoid, NumberStyles.None, CultureInfo.InvariantCulture))], written, false);
        }

        return value;
    }

    /// <summary>
    /// Checks a numeric value an update writes into a column that uses managed sequences, where it changes the
    /// column, and gives the value back, as <see cref="CheckedValue"/> does a bigint.
    /// </summary>
    /// <param name="sequences">The sequences the column uses, as their OIDs with commas between.</param>
    /// <param name="was">The value the row held.</param>
    /// <param name="value">The value the update sets.</param>
    /// <returns>The value the update sets; one outside this node's blocks fails.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_checked(sequences text, was numeric, value numeric)
        RETURNS numeric VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static PgNumeric? CheckedNumeric(string? sequences, PgNumeric? was, PgNumeric? value)
    {
        if (sequences is not null && value is PgNumeric written && (was is not PgNumeric before || !Same(before, written)))
        {
            CheckUnder([.. sequences.Split(',').Select(seqoid => uint.Parse(seqoid, NumberStyles.None, CultureInfo.InvariantCulture))], written, false);
        }

        return value;
    }

    // Whether two numeric values are the same number, as IS NOT DISTINCT FROM has them.
    private static bool Same(PgNumeric one, PgNumeric other)
        => Whole(one) is decimal first && Whole(other) is decimal second ? first == second : one.Text == other.Text;

    // Whether a value lies in a block of a sequence, without failing, by the blocks as they now stand.
    private static bool AdmitsReloaded(uint seqoid, long value)
    {
        Reload();
        return !s_moved.Contains(seqoid) && Admits(seqoid, value);
    }

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
        => AdmitsReloaded((uint)sequence.DangerousGetBits(), value);

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
        // A statement one of whose calls of nextval names no sequence has its rows count the sequences the transaction
        // draws from down as they're seen to draw from them. The sequences its calls name are checked as its rows start.
        if (s_drawingUngated.Count > 0 && queryDesc->plannedstmt is not null)
        {
            (HashSet<uint> called, int calls, bool unnamed) = StatementDraws(queryDesc);
            s_readDraws = ((nint)queryDesc->plannedstmt, (called, calls, unnamed));
            if (unnamed)
            {
                StartUnnamed(called, calls);
            }
        }

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
    /// Reads the calls of nextval a statement's plan makes, kept by the statement's text, the search path and the role it
    /// runs as, until a table, a type, a constraint, a function or a schema changes, or the managed sequences do, so that
    /// a statement run again doesn't write its plan out again, as it runs or as a call of nextval of its starts, of a
    /// sequence worf manages or not. Of a statement with parameters, whose values a plan may
    /// hold, as the sequence a call names, only that it makes no call is kept, and only when neither its text nor a
    /// function of a user's that it calls, which the planner may inline as the values have it, may call nextval: its
    /// calls come from its text, its views and the functions it calls, which its values change none of. The calls of
    /// an INSERT of the rows it is given are kept too, on the same terms: they are its columns' defaults'.
    /// </summary>
    private static unsafe (HashSet<uint> Called, int Calls, bool Unnamed) StatementDraws(QueryDesc* queryDesc)
        => PlanDraws((nint)queryDesc->plannedstmt, (nint)queryDesc->sourceText, queryDesc->@params is not null && queryDesc->@params->numParams > 0);

    // Reads the calls of nextval a plan makes, as StatementDraws keeps them: for a statement that runs, and for one whose
    // plan starts, which every call of nextval that starts asks of, whichever sequence it draws from.
    private static unsafe (HashSet<uint> Called, int Calls, bool Unnamed) PlanDraws(nint statement, nint source, bool parameters)
    {
        PlannedStmt* planned = (PlannedStmt*)statement;
        if (source == 0)
        {
            return Draws(PlanText((Node*)planned));
        }

        WatchTables();
        string text = StatementText(source, planned->stmt_location, planned->stmt_len);
        (string Text, string Path, uint User) key = (text, SearchPath(), NativeMethods.GetUserId());
        if (s_drawsByText.TryGetValue(key, out (HashSet<uint> Called, int Calls, bool Unnamed) draws))
        {
            return draws;
        }

        draws = Draws(PlanText((Node*)planned));
        WorfLog.Debug1("read a statement's plan for its calls of nextval");
        if (!parameters || (!text.Contains("nextval", StringComparison.OrdinalIgnoreCase) && !CallsUserFunctions(planned) &&
            (draws is { Calls: 0, Unnamed: false } || InsertsRowsAsGiven(planned))))
        {
            if (s_drawsByText.Count >= 1000)
            {
                s_drawsByText.Clear();
            }

            s_drawsByText[key] = draws;
        }

        return draws;
    }

    // Whether a plan is an INSERT's that writes the rows one node with nothing under it gives, as a VALUES list's: its
    // calls of nextval are then those of the columns' defaults, whatever values its parameters hold.
    private static unsafe bool InsertsRowsAsGiven(PlannedStmt* planned)
    {
        if (planned->commandType != CmdType.CMD_INSERT || planned->hasModifyingCTE || planned->subplans is not null || planned->planTree is null ||
            planned->planTree->type != NodeTag.T_ModifyTable)
        {
            return false;
        }

        Plan* rows = planned->planTree->lefttree;
        return rows is not null && rows->lefttree is null && rows->righttree is null && rows->initPlan is null &&
            rows->type is NodeTag.T_Result or NodeTag.T_ValuesScan;
    }

    // Whether a plan calls a function of a user's, which it depends on, as one the planner inlined.
    private static unsafe bool CallsUserFunctions(PlannedStmt* planned)
    {
#if ANKUS_PG19
        const int procedures = (int)SysCacheIdentifier.PROCOID;
#else
        const int procedures = ProcedureCacheId;
#endif
        List* items = planned->invalItems;
        for (int i = 0; items is not null && i < items->length; i++)
        {
            if (((PlanInvalItem*)items->elements[i].ptr_value)->cacheId == procedures)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Forgets a statement as it ends, so that a statement that runs later with the same command ID, as one of the
    /// extended protocol's portals does after another's partial execution, isn't taken for it.
    /// </summary>
    /// <param name="queryDesc">The statement.</param>
    internal static unsafe void EndStatement(QueryDesc* queryDesc)
    {
        // A statement that went by what the guard kept of it left nothing but its brackets.
        if (s_settledRunning.Count != 0 && s_settledRunning.Remove((nint)queryDesc))
        {
            ReturnBrackets((nint)queryDesc);
            return;
        }

        FreePlanSites((nint)queryDesc);
        if (s_routing.Count > 0 && s_routing.RemoveAll(routed => routed.Query == (nint)queryDesc) > 0)
        {
            _ = s_routedMarked.Remove((nint)queryDesc->estate);
        }

        _ = s_planStarted.Remove((nint)queryDesc);
        _ = s_runStarted.Remove((nint)queryDesc);
        _ = s_runFound.Remove((nint)queryDesc);
        if (s_valuesRows.Remove((nint)queryDesc))
        {
            s_valuesRow = default;
        }

        _ = s_runTop.Remove((nint)queryDesc);
        _ = s_rowsDrawing.Remove((nint)queryDesc);
        if (s_hookingStatements.Count > 0)
        {
            _ = s_hookingStatements.RemoveAll(entry => entry.Statement == (nint)queryDesc);
        }

        ForgetCounted((nint)queryDesc);
        EndDrawing((nint)queryDesc);

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

        // An INSERT whose rows take what the guard's trigger checks from the columns' own defaults writes nothing for
        // the trigger to see: each value is one its sequence has just issued. Such a statement takes no gate, its
        // calls go through the guard one by one, as those of a SELECT of nextval do, and its rows skip the trigger.
        s_identitiesRead.Clear();
        if (!s_gatingAfterAll && TakesDefaults(queryDesc, written, s_identitiesRead, out bool routes, out short verifies))
        {
            s_takingDefaults[(nint)queryDesc] = (written, s_identitiesRead.Count == 0 ? s_noIdentities : [.. s_identitiesRead], routes, verifies);
            return;
        }

        // An UPDATE, or a MERGE's, draws only as it sets a column to its default, which an identity column draws from
        // without calling nextval, so it holds the gates of the sequences of the columns it sets that its plan draws from,
        // and as its tables' update triggers, or their partitions' insert triggers, which fire as a row moves to another
        // partition, draw from the sequences they name. An insert draws from those its tables' defaults draw from, and
        // those their triggers, and their partitions', name, whether or not a guard checks the tables.
        List<uint> sequences = inserts ? [.. written.SelectMany(DrawnBy)] : [];
        if (updates && !upserts && written.Exists(Guarded))
        {
            sequences.AddRange(UpdatedDrawing(statement));
        }

        // The trigger functions whose C code may draw from the sequences their triggers name, as contrib's autoinc does,
        // which no call worf sees makes, check those sequences as each call starts, in the fmgr hook, whatever statement
        // fires them, a data-modifying WITH's too: an update trigger fires only for a row the statement updates, as an
        // upsert's for a row it conflicts with, so the statement waits for none of its sequences until it does.
        Dictionary<uint, HashSet<uint>> functions = [];
        bool modifyingCte = statement->hasModifyingCTE;
        foreach (uint table in written.Where(table => Triggers(table).Argued))
        {
            List<Dictionary<uint, List<uint>>> fired = [];
            if (inserts)
            {
                fired.Add(ArguedFunctions(table, TriggerTypeInsert));
            }

            if (updates || modifyingCte)
            {
                fired.Add(ArguedFunctions(table, TriggerTypeUpdate, upserts ? 0 : TriggerTypeInsert));
            }

            foreach ((uint function, List<uint> named) in fired.SelectMany(each => each))
            {
                functions[function] = [.. functions.GetValueOrDefault(function, []).Union(named)];
            }
        }

        List<uint> triggered = [.. functions.Values.SelectMany(named => named).Where(seqoid => !sequences.Contains(seqoid)).Distinct()];
        if (sequences.Count == 0 && triggered.Count == 0)
        {
            return;
        }

        HoldOffWorfDrop();
        OpenGates((nint)queryDesc, [.. sequences, .. triggered]);
        (int calls, bool unseen) = WrittenCalls(written);

        // A statement whose plan calls a function of a user's, as a default may, draws as that function does, which its
        // text doesn't show.
        unseen |= CallsUserFunctions(statement);
        long rowDraws = 1 + calls;
        foreach (uint seqoid in sequences.Concat(triggered))
        {
            // A row whose triggers may draw more than their source shows counts, until this session has seen a row draw,
            // for as many as a row's draws are counted for at most, and then for as many as it was seen to draw. One whose
            // source shows every draw counts for those alone, as other sessions' draws meanwhile move the sequence too.
            if (unseen)
            {
                _ = s_unseenRowDraws.Add(seqoid);
            }

            long seen = !unseen ? 0 : s_seenRowDraws.TryGetValue(seqoid, out long drawn) ? drawn : RowDrawsUnknown(seqoid);
            s_gatedRowDraws[seqoid] = Math.Max(s_gatedRowDraws.GetValueOrDefault(seqoid), Math.Max(rowDraws, seen));
            _ = s_seenThisStatement.Remove(seqoid);
        }

        if (functions.Count > 0)
        {
            HookTriggers((nint)queryDesc, functions);
        }

        WaitBeforeDrawing(sequences);
        foreach (uint seqoid in sequences.Concat(triggered))
        {
            MarkRow(seqoid);
        }

        if (sequences.Count > 0)
        {
            s_rowsDrawing[(nint)queryDesc] = sequences;
        }
    }

    // The calls of nextval the plan of the statement starting makes, as its start read them.
    private static (nint Statement, (HashSet<uint> Called, int Calls, bool Unnamed) Draws) s_readDraws;

    // The statements starting whose rows take the guarded columns' values from the columns' defaults, with the tables
    // they write and the sequences of the identity columns among those columns.
    private static readonly Dictionary<nint, (List<uint> Written, List<uint> Identities, bool Routes, short Verifies)> s_takingDefaults = [];

    // Whether a statement that set out to take its defaults without a gate takes its gates after all.
    private static bool s_gatingAfterAll;

    // The identity columns' sequences read of the statement starting, and the list of none.
    private static readonly List<uint> s_identitiesRead = [];
    private static readonly List<uint> s_noIdentities = [];

    // What the guard's insert trigger checks of each table: whether a row can take what it checks from the defaults
    // alone, and the columns it checks, with the sequences it checks each under. Read once, until a table changes.
    private static readonly Dictionary<uint, (bool Plain, List<(short Column, uint[] Sequences)> Columns, bool Routes, bool Changed)> s_checked = [];

    // Whether an INSERT writes one table whose rows hold, in every column the guard's trigger checks, the value the
    // column's own default draws from a managed sequence the trigger checks it under, which it then needn't check:
    // the plan gives each such column a call of nextval that names the sequence, under no more than a cast, no row
    // trigger runs before the row is written, which might change it, and no trigger's arguments name a sequence.
    private static unsafe bool TakesDefaults(QueryDesc* queryDesc, List<uint> written, List<uint> identities, out bool routes, out short verifies)
    {
        routes = false;
        verifies = 0;
        PlannedStmt* statement = queryDesc->plannedstmt;
        if (queryDesc->operation is not (CmdType.CMD_INSERT or CmdType.CMD_MERGE) || statement->hasModifyingCTE || written.Count != 1 ||
            statement->planTree is null || statement->planTree->type != NodeTag.T_ModifyTable)
        {
            return false;
        }

        ModifyTable* modify = (ModifyTable*)statement->planTree;
        Plan* rows = modify->plan.lefttree;
        if (modify->rootRelation != 0 || rows is null || modify->resultRelations is null || modify->resultRelations->length != 1)
        {
            return false;
        }

        uint relid = ((RangeTblEntry*)statement->rtable->elements[modify->resultRelations->elements[0].int_value - 1].ptr_value)->relid;
        (bool plain, List<(short Column, uint[] Sequences)> columns, routes, bool changed) = Checked(relid);
        if (relid != written[0] || (!plain && !changed) || columns.Count == 0)
        {
            return false;
        }

        // A row trigger that runs before the insert may change the value a row drew, so the guard's trigger stays,
        // with a condition that first sets the row's value against the one its default drew: an INSERT into a table
        // that the guard checks in one column of whole numbers, under one sequence, has it.
        if (changed)
        {
            if (queryDesc->operation != CmdType.CMD_INSERT || columns.Count != 1 || columns[0].Sequences.Length != 1 || !ComparedDirectly(relid, columns[0].Column))
            {
                return false;
            }

            verifies = columns[0].Column;
        }

        // Whether each checked column of the rows a list of values gives takes its own default's draw.
        bool Taken(List* values)
        {
            foreach ((short column, uint[] sequences) in columns)
            {
                Node* value = null;
                for (int i = 0; values is not null && i < values->length; i++)
                {
                    TargetEntry* entry = (TargetEntry*)values->elements[i].ptr_value;
                    if (entry->resno == column && !entry->resjunk)
                    {
                        value = Uncast((Node*)entry->expr);
                        break;
                    }
                }

                if (!DrawsFrom(value, sequences, identities))
                {
                    return false;
                }
            }

            return true;
        }

        // Whether an update sets none of the checked columns, by the columns it sets.
        bool Kept(List* set)
        {
            for (int i = 0; set is not null && i < set->length; i++)
            {
                if (columns.Exists(column => column.Column == set->elements[i].int_value))
                {
                    return false;
                }
            }

            return true;
        }

        // The rows an ON CONFLICT DO UPDATE or a MERGE updates keep the values the guard passed before, where the
        // update sets none of the checked columns and no row trigger before it, which could, runs: the update
        // guard's own condition checks a row such a trigger changed.
        bool updates = false;
        if (queryDesc->operation == CmdType.CMD_INSERT)
        {
            updates = modify->onConflictAction == OnConflictAction.ONCONFLICT_UPDATE;
            if (!Taken(rows->targetlist) || (updates && !Kept(modify->onConflictCols)))
            {
                return false;
            }
        }
        else
        {
            List* actions = modify->mergeActionLists is null || modify->mergeActionLists->length != 1 ? null : (List*)modify->mergeActionLists->elements[0].ptr_value;
            bool inserts = false;
            for (int i = 0; actions is not null && i < actions->length; i++)
            {
                MergeAction* action = (MergeAction*)actions->elements[i].ptr_value;
                if (action->commandType == CmdType.CMD_INSERT)
                {
                    inserts = true;
                    if (!Taken(action->targetList))
                    {
                        return false;
                    }
                }
                else if (action->commandType == CmdType.CMD_UPDATE)
                {
                    updates = true;
                    if (!Kept(action->updateColnos))
                    {
                        return false;
                    }
                }
            }

            if (!inserts)
            {
                return false;
            }
        }

        // A trigger whose arguments name a sequence may draw from it in C code, which only a gate held makes safe.
        return ArguedFunctions(relid, TriggerTypeInsert).Count == 0 && (!updates || ArguedFunctions(relid, TriggerTypeUpdate).Count == 0);
    }

    // Whether a column's values are whole numbers PostgreSQL holds by value, which the guard's condition compares
    // with the value a row drew as they stand.
    private static bool ComparedDirectly(uint relid, short column)
    {
        const uint Int2Oid = 21;
        const uint Int4Oid = 23;
        unsafe
        {
            return NativeMethods.get_atttype(relid, column) is Int2Oid or Int4Oid or BigintOid;
        }
    }

    // Whether an expression is a call of nextval that names, with a constant, one of some sequences, each managed and
    // this node's own to draw from, or the draw of an identity column from one, which is no call of nextval: worf
    // then has it run as one, where its calls go through the fmgr hook, and the sequence is noted.
    private static unsafe bool DrawsFrom(Node* value, uint[] sequences, List<uint> identities)
    {
        if (value is null)
        {
            return false;
        }

        uint named = 0;
        if (value->type == NodeTag.T_FuncExpr && ((FuncExpr*)value)->funcid == NextvalOid && NamesRegclass(value))
        {
            named = NamedByCall(value);
        }
        else if (value->type == NodeTag.T_NextValueExpr && HooksCalls())
        {
            named = ((NextValueExpr*)value)->seqid;
            identities.Add(named);
        }

        return named != 0 && Array.IndexOf(sequences, named) >= 0 && s_sequences.ContainsKey(named) && !s_moved.Contains(named);
    }

    // Whether a call of nextval can go through the fmgr hook, within PostgreSQL's own call.
    private static bool HooksCalls()
    {
        if (!Executor.CountsDraws)
        {
            return false;
        }

        ReadNextval();
        ReadHookedCall();
        return s_hookedCall != 0;
    }

    // Has the draws of the identity columns of a statement whose rows take their defaults run as calls of nextval that
    // go through the guard, and tells whether it found one for each of the columns' sequences. PostgreSQL draws for
    // an identity column in a step of its own, which calls no function, so the step becomes the call's: a call of
    // nextval that names the sequence is started beside it, and its step's code and what it holds take the draw's
    // place. The call runs as the bootstrap superuser, as fmgr_security_definer runs a function whose state names
    // an owner, since an identity column asks its writer for no privilege on its sequence.
    private static unsafe bool HookIdentityDraws(QueryDesc* query, WorfStartingStatement starting, List<uint> identities)
    {
        PlanState* rows = query->planstate is null ? null : query->planstate->lefttree;
        if (identities.Count == 0 || rows is null || query->estate is null)
        {
            return identities.Count == 0;
        }

        // A MERGE's rows draw as its INSERT actions work them out, in the table it writes.
        List<(nint Step, bool Single, bool Compiled)> draws = [];
        Dictionary<nint, nint> states = [];
        s_findingIdentities = true;
        s_statesOf = states;
        try
        {
            FindCalls(query->operation == CmdType.CMD_MERGE ? query->planstate : rows, draws, []);
        }
        finally
        {
            s_findingIdentities = false;
            s_statesOf = null;
        }

        if (!s_planSites.TryGetValue((nint)query, out List<(int Site, nint Call, nint Information)>? sites))
        {
            sites = [];
        }

        s_snowflake = Worf.CatalogOid("snowflake_node");
        HashSet<uint> hooked = [];
        List<(nint Step, uint Seqoid, ExprEvalStep Own)> bracketed = [];
        foreach ((nint draw, bool single, bool compiled) in draws)
        {
            ExprEvalStep* step = (ExprEvalStep*)draw;
            uint seqoid = step->d.nextvalueexpr.seqid;
            ExprEvalStep own = *step;
            if (compiled || !identities.Contains(seqoid) || !HookDraw(step, query->estate->es_query_cxt))
            {
                continue;
            }

            StartHooked(starting, seqoid);
            int site = GuardCall(step);
            if (single)
            {
                _ = s_singleCalls.Add((nint)step->d.func.fcinfo_data);
            }

            if (site >= 0)
            {
                bracketed.Add((draw, seqoid, own));
            }

            sites.Add((site, (nint)step->d.func.finfo->fn_expr, (nint)step->d.func.fcinfo_data));
            _ = hooked.Add(seqoid);
        }

        BracketDraws((nint)query, bracketed, states, identities: true);

        if (sites.Count > 0)
        {
            RegisterEnd();
            HookExit();
            s_planSites[(nint)query] = sites;
        }

        return identities.TrueForAll(hooked.Contains);
    }

    // Whether steps that draw for identity columns are looked for, rather than calls of nextval.
    private static bool s_findingIdentities;

    // What a step that calls nextval holds, read once for the backend from a call started for it: the step's code,
    // as the interpreter knows it, what PostgreSQL looked up for the function, and the call's expression.
    private static bool s_drawRead;
    private static long s_drawCode;
    private static FmgrInfo s_drawFunction;
    private static nint s_drawCall;

    // The expression of the call each identity column's sequence is drawn from with, by sequence.
    private static readonly Dictionary<uint, nint> s_drawCalls = [];

    // Starts a call of nextval that names a sequence with a constant, as anyone may, to read what its step holds.
    private static unsafe bool ReadDraw()
    {
        if (s_drawRead)
        {
            return s_drawCode != 0;
        }

        uint userId;
        int secContext;
        NativeMethods.GetUserIdAndSecContext(&userId, &secContext);
        bool naming = s_namingGrantors;
        s_namingGrantors = true;
        try
        {
            NativeMethods.SetUserIdAndSecContext(BootstrapSuperuserId, secContext | SecurityLocalUseridChange);
            MemoryContextData* old = Worf.MemoryContextSwitchTo(NativeGlobals.TopMemoryContext);
            FuncExpr* call;
            try
            {
                Const* named = NativeMethods.makeConst(RegclassOid, -1, 0, sizeof(uint), 0, false, true);
                call = NativeMethods.makeFuncExpr(NextvalOid, BigintOid, NativeMethods.lappend(null, named), 0, 0, CoercionForm.COERCE_EXPLICIT_CALL);
            }
            finally
            {
                _ = Worf.MemoryContextSwitchTo(old);
            }

            ExprState* started = NativeMethods.ExecInitExpr((Expr*)call, null);
            ExprEvalStep* draw = started->steps_len > 0 ? &started->steps[0] : null;
            if (draw is not null && (started->flags & InterpreterInitialized) != 0 && CallsNextval(started, draw))
            {
                s_drawFunction = *draw->d.func.finfo;
                s_drawCall = (nint)call;
                s_drawCode = draw->opcode;
            }

            s_drawRead = true;
        }
        finally
        {
            s_namingGrantors = naming;
            NativeMethods.SetUserIdAndSecContext(userId, secContext);
        }

        return s_drawCode != 0;
    }

    // The room left for what fmgr_security_definer keeps of a function it runs, in the piece a draw's call is given.
    private const int DrawCacheSize = 128;

    // Turns a step that draws for an identity column into a call of nextval that names the column's sequence, and
    // tells whether it did. One piece of some memory holds what the step's call needs: what PostgreSQL looks up for
    // the function, what fmgr_security_definer keeps of a function it runs, which begins with that lookup and the
    // role to run it as on every version and which zeros leave empty, and the call's argument.
    private static unsafe bool HookDraw(ExprEvalStep* step, MemoryContextData* memory)
    {
        const int CacheSize = DrawCacheSize;
        if (!ReadDraw())
        {
            return false;
        }

        // The call's expression, which names the sequence, is one for all the sequence's draws, kept for the backend:
        // two of them hooked at once, as a row's trigger writing the same table has, share its place among the calls
        // that go through the hook, which sends the outer one's draws into worf proper.
        uint seqoid = step->d.nextvalueexpr.seqid;
        if (!s_drawCalls.TryGetValue(seqoid, out nint named))
        {
            MemoryContextData* top = Worf.MemoryContextSwitchTo(NativeGlobals.TopMemoryContext);
            try
            {
                FuncExpr* made = (FuncExpr*)NativeMethods.copyObjectImpl((void*)s_drawCall);
                ((Const*)made->args->elements[0].ptr_value)->constvalue = seqoid;
                named = (nint)made;
            }
            finally
            {
                _ = Worf.MemoryContextSwitchTo(top);
            }

            s_drawCalls[seqoid] = named;
        }

        int callSize = sizeof(FunctionCallInfoBaseData) + (2 * sizeof(ulong));
        MemoryContextData* old = Worf.MemoryContextSwitchTo(memory);
        try
        {
            byte* piece = (byte*)NativeMethods.MemoryContextAllocZero(memory, (nuint)(sizeof(FmgrInfo) + CacheSize + callSize));
            FuncExpr* call = (FuncExpr*)named;
            FmgrInfo* function = (FmgrInfo*)piece;
            byte* cache = piece + sizeof(FmgrInfo);
            FunctionCallInfoBaseData* arguments = (FunctionCallInfoBaseData*)(cache + CacheSize);
            *function = s_drawFunction;
            function->fn_mcxt = memory;
            function->fn_expr = (Node*)call;
            function->fn_extra = null;
            *(FmgrInfo*)cache = *function;
            *(uint*)(cache + sizeof(FmgrInfo)) = BootstrapSuperuserId;
            function->fn_extra = cache;
            arguments->flinfo = function;
            arguments->nargs = 1;
            *(ulong*)((byte*)arguments + sizeof(FunctionCallInfoBaseData)) = seqoid;
            step->opcode = s_drawCode;
            step->d.func.finfo = function;
            step->d.func.fcinfo_data = arguments;
            step->d.func.fn_addr = s_drawFunction.fn_addr;
            step->d.func.nargs = 1;
            return true;
        }
        finally
        {
            _ = Worf.MemoryContextSwitchTo(old);
        }
    }

    // Notes, for a draw worf turned into a call of nextval, what the call's start would have: the statement draws
    // from the sequence without its gate, waits for its grantor, and is counted among those drawing from it, its
    // check left to its first row.
    private static void StartHooked(WorfStartingStatement starting, uint seqoid)
    {
        if (starting.Explained || StatementGated(seqoid) || !starting.Switched.Add(seqoid))
        {
            return;
        }

        if (s_grantors.ContainsKey(seqoid) && !s_confirmedSequences.Contains(seqoid))
        {
            WaitForGrantor(seqoid);
        }

        List<uint> drawn = [seqoid];
        if (Within() && s_drewWithin.Contains(seqoid))
        {
            CountFunction(drawn);
        }
        else
        {
            CountUngated(drawn);
        }

        NoteDrawsWithin(drawn);
        DeferUngated(drawn);
    }

    // The expression under the casts that give a value its column's type.
    private static unsafe Node* Uncast(Node* value)
    {
        while (value is not null)
        {
            if (value->type == NodeTag.T_RelabelType)
            {
                value = (Node*)((RelabelType*)value)->arg;
            }
            else if (value->type == NodeTag.T_CoerceToDomain)
            {
                value = (Node*)((CoerceToDomain*)value)->arg;
            }
            else if (value->type == NodeTag.T_FuncExpr && ((FuncExpr*)value)->funcformat != CoercionForm.COERCE_EXPLICIT_CALL &&
                ((FuncExpr*)value)->funcid != NextvalOid && ((FuncExpr*)value)->args is not null && ((FuncExpr*)value)->args->length >= 1)
            {
                value = (Node*)((FuncExpr*)value)->args->elements[0].ptr_value;
            }
            else
            {
                break;
            }
        }

        return value;
    }

    // Reads what the guard's insert trigger checks of a table, from the arguments worf gave it: each names a column
    // by number and the sequences it is checked under, by OID, or by a name a default gives as text, which only the
    // trigger can look up as the row is written. Only a table that is no partition, with no row trigger before or
    // instead of its inserts, can have rows that take what the trigger checks from the defaults alone; one that is
    // partitioned routes its rows to partitions, none of which may have such a trigger either, or a column of its
    // own that the guard checks.
    private static (bool Plain, List<(short Column, uint[] Sequences)> Columns, bool Routes, bool Changed) Checked(uint relid)
    {
        _ = Triggers(relid);
        if (s_checked.TryGetValue(relid, out (bool Plain, List<(short Column, uint[] Sequences)> Columns, bool Routes, bool Changed) found))
        {
            return found;
        }

        // An ordinary table's triggers are read from its own entry in PostgreSQL's cache, which the statement that
        // writes it has loaded, so no catalog is read for them.
        if (CachedChecked(relid) is (string kept, bool alone, bool changes))
        {
            found = kept.Length == 0 ? (false, [], false, false) : Checked(kept, alone, false, changes);
            s_checked[relid] = found;
            return found;
        }

        // Those of a partitioned table, and of its partitions, are read straight from pg_trigger. The table is
        // plain where it is no partition, no table of its tree holds a partition's own guard, and no partition has
        // a row trigger that runs before an insert or instead of it.
        const int Row = 1 << 0;
        const int Before = 1 << 1;
        const int Instead = 1 << 6;
        sbyte kind;
        bool partition;
        unsafe
        {
            kind = NativeMethods.get_rel_relkind(relid);
            partition = NativeMethods.get_rel_relispartition(relid);
        }

        static bool Changes((uint Table, string Name, uint Function, int Type, char Enabled, bool Internal, string[] Arguments, string? Condition) trigger)
            => !trigger.Internal && (trigger.Type & Row) != 0 && (trigger.Type & TriggerTypeInsert) != 0 && (trigger.Type & (Before | Instead)) != 0;

        List<(uint Table, string Name, uint Function, int Type, char Enabled, bool Internal, string[] Arguments, string? Condition)> triggers = TreeTriggers(relid);
        string[]? given = triggers.Where(trigger => trigger.Table == relid && trigger.Name is SequenceRanges.GuardName or SequenceRanges.UpdateGuardName)
            .OrderBy(static trigger => trigger.Name, StringComparer.Ordinal).Select(static trigger => trigger.Arguments).FirstOrDefault();
        found = given is null ? (false, [], false, false)
            : Checked(string.Concat(given.Select(static argument => argument + "\\000")),
                kind is (sbyte)'r' or (sbyte)'p' && !partition && !triggers.Exists(static trigger => trigger.Name == SequenceRanges.PartitionGuardName) &&
                    !triggers.Exists(trigger => trigger.Table != relid && Changes(trigger)),
                kind == (sbyte)'p', triggers.Exists(trigger => trigger.Table == relid && Changes(trigger)));
        s_checked[relid] = found;
        return found;
    }

    // What the guard's trigger's arguments, with a zero byte after each as PostgreSQL's escape form writes one, say
    // it checks, with what was read of the table's other triggers.
    private static (bool Plain, List<(short Column, uint[] Sequences)> Columns, bool Routes, bool Changed) Checked(string arguments, bool plain, bool routes,
        bool changed)
    {
        // A row trigger of the table's own that runs before an insert, or instead of it, may change the row: its
        // rows then can't skip the guard, though the guard can tell a row such a trigger left alone.
        List<(short Column, uint[] Sequences)> columns = [];
        foreach (string argument in arguments.Split("\\000", StringSplitOptions.RemoveEmptyEntries))
        {
            int colon = argument.IndexOf(':', StringComparison.Ordinal);
            if (colon <= 0 || !short.TryParse(argument.AsSpan(0, colon), NumberStyles.None, CultureInfo.InvariantCulture, out short column))
            {
                continue;
            }

            List<uint> sequences = [];
            foreach (string named in argument[(colon + 1)..].Split(','))
            {
                if (uint.TryParse(named, NumberStyles.None, CultureInfo.InvariantCulture, out uint seqoid))
                {
                    sequences.Add(seqoid);
                }
                else
                {
                    plain = false;
                }
            }

            columns.Add((column, [.. sequences]));
        }

        return (plain && !changed, columns, routes, plain && changed && !routes);
    }

    // Reads what Checked asks of an ordinary table from the table's own entry in PostgreSQL's cache: the guard
    // trigger's arguments, whether the table stands alone, as no partition does, and holds no partition's own guard,
    // and whether a row trigger of its own runs before an insert or instead of it, with no arguments for a table
    // the guard doesn't check. Null for a partitioned table, whose partitions' triggers count too.
    private static unsafe (string Arguments, bool Alone, bool Changed)? CachedChecked(uint relid)
    {
        const int Row = 1 << 0;
        const int Before = 1 << 1;
        const int Instead = 1 << 6;
        RelationData* table = NativeMethods.relation_open(relid, (int)PgLockMode.None);
        try
        {
            if (table->rd_rel->relkind != (sbyte)'r')
            {
                return null;
            }

            Trigger* guard = null;
            bool alone = !table->rd_rel->relispartition;
            bool changed = false;
            for (int i = 0; table->trigdesc is not null && i < table->trigdesc->numtriggers; i++)
            {
                Trigger* trigger = &table->trigdesc->triggers[i];
                ReadOnlySpan<byte> name = MemoryMarshal.CreateReadOnlySpanFromNullTerminated((byte*)trigger->tgname);
                if (name.SequenceEqual(GuardNameBytes) || (guard is null && name.SequenceEqual(UpdateGuardNameBytes)))
                {
                    guard = trigger;
                }

                alone &= !name.SequenceEqual("worf_sequence_range_partition_guard"u8);
                changed |= !trigger->tgisinternal && (trigger->tgtype & Row) != 0 && (trigger->tgtype & TriggerTypeInsert) != 0 &&
                    (trigger->tgtype & (Before | Instead)) != 0;
            }

            if (guard is null)
            {
                return ("", alone, changed);
            }

            StringBuilder arguments = new();
            for (int i = 0; i < guard->tgnargs; i++)
            {
                _ = arguments.Append(Worf.FromServer(guard->tgargs[i])).Append("\\000");
            }

            return (arguments.ToString(), alone, changed);
        }
        finally
        {
            NativeMethods.relation_close(table, (int)PgLockMode.None);
        }
    }

    // The statements running whose rows take the guarded columns' values from the columns' defaults and go to
    // partitions, with the subtransactions they started in, and the executor state of a COPY of such rows, with how
    // deep the scopes around its rows stand and how many COPYs deep it runs. A partition's copy of the guard's trigger is made as its first row
    // comes, so the trigger is skipped from the row after: as PostgreSQL starts the trigger's condition, worf marks
    // it disabled in the partition's copy, and the check that first row runs passes it unread.
    private static readonly List<(nint Query, uint Subxact)> s_routing = [];
    private static (nint State, int Depth, int Copies) s_copyRouting;

    /// <summary>
    /// Gets a value indicating whether a statement or COPY under way routes rows that take their defaults to
    /// partitions, whose copies of the guard's trigger are to be skipped as they're made.
    /// </summary>
    internal static bool RoutesDefaults => s_routing.Count > 0 || s_copyRouting.State != 0;

    /// <summary>
    /// Has the partitions that the statements and COPY routing rows that take their defaults have opened so far skip
    /// the guard's trigger, as a function of some expression starts, which the trigger's condition in a partition
    /// newly opened does.
    /// </summary>
    internal static unsafe void SkipRoutedGuards()
    {
        foreach ((nint routed, _) in s_routing)
        {
            SkipRoutedGuards(((QueryDesc*)routed)->estate);
        }

        if (s_copyRouting.State != 0)
        {
            SkipRoutedGuards((EState*)s_copyRouting.State);
        }
    }

    private static unsafe void SkipRoutedGuards(EState* state)
    {
        if (state is null || state->es_tuple_routing_result_relations is null)
        {
            return;
        }

        // Each partition is marked once, as the first function after it opens starts.
        int opened = state->es_tuple_routing_result_relations->length;
        for (int i = s_routedMarked.GetValueOrDefault((nint)state); i < opened; i++)
        {
            SkipGuard((ResultRelInfo*)state->es_tuple_routing_result_relations->elements[i].ptr_value);
        }

        s_routedMarked[(nint)state] = opened;
    }

    // How many of the partitions each executor state has opened have had the guard's trigger marked skipped.
    private static readonly Dictionary<nint, int> s_routedMarked = [];

    // Whether the row whose value the guard's trigger is checking is one of a statement's or a COPY's that takes the
    // value from the column's default: the innermost statement running is such a statement, or no statement has
    // started within such a COPY.
    private static bool ChecksDefaultTaken()
    {
        if (s_copyRouting.State != 0 && s_scopes.Count == s_copyRouting.Depth)
        {
            return true;
        }

        for (int i = s_scopes.Count - 1; i >= 0 && s_routing.Count > 0; i--)
        {
            if (s_scopes[i].Kind == RunScope)
            {
                nint run = s_scopes[i].Key;
                return s_routing.Exists(routed => routed.Query == run);
            }
        }

        return false;
    }

    // Has the rows of an INSERT that take the guarded columns' values from their defaults skip the guard's trigger:
    // each table a statement writes keeps its own copy of the table's triggers, in which the trigger is marked
    // disabled, and where no other trigger fires after a row's insert, PostgreSQL then looks at none for it.
    private static unsafe void SkipGuard(ResultRelInfo* written)
    {
        const int Row = 1 << 0;
        const int Before = 1 << 1;
        const int Instead = 1 << 6;
        const sbyte Disabled = (sbyte)'D';
        TriggerDesc* triggers = written->ri_TrigDesc;
        if (triggers is null)
        {
            return;
        }

        bool others = false;
        for (int i = 0; i < triggers->numtriggers; i++)
        {
            Trigger* trigger = &triggers->triggers[i];
            if ((trigger->tgtype & (Row | Before | TriggerTypeInsert | Instead)) != (Row | TriggerTypeInsert))
            {
                continue;
            }

            if (MemoryMarshal.CreateReadOnlySpanFromNullTerminated((byte*)trigger->tgname).SequenceEqual(GuardNameBytes))
            {
                trigger->tgenabled = Disabled;
            }
            else
            {
                others = true;
            }
        }

        if (!others)
        {
            triggers->trig_insert_after_row = false;
        }
    }

    private static ReadOnlySpan<byte> GuardNameBytes => "worf_sequence_range_guard"u8;

    private static ReadOnlySpan<byte> UpdateGuardNameBytes => "worf_sequence_range_guard_update"u8;

    // The conditions built for the tables whose guard triggers hold none, by table, and the memory of those a change
    // to a table has retired, which a statement that runs them may hold until its transaction ends.
    private static readonly Dictionary<uint, WorfGuardConditions> s_conditions = [];
    private static readonly List<nint> s_retiredConditions = [];
    private static bool s_retiringConditions;

    // Whether each function looked up so far is the guard's trigger function, until a function changes, and what
    // marks a trigger's copy as holding a condition, which PostgreSQL then runs as the statement built it and never
    // reads.
    private static readonly Dictionary<uint, bool> s_guardFunctions = [];
    private static nint s_conditionMark;

    // The names a trigger's condition knows a row's old and new versions by.
    private static readonly string[] s_rowAliases = ["old", "new"];

    // INNER_VAR and OUTER_VAR, from nodes/primnodes.h, which stand for a row's old and new versions in a condition.
    private const int OldRowVar = -1;
    private const int NewRowVar = -2;

    /// <summary>
    /// Gets whether a trigger is one of the guard's that holds no condition of its own, which worf's own apply and
    /// sync workers have their writers skip: the rows they write are other nodes', which the guard leaves alone.
    /// </summary>
    /// <param name="trigger">The trigger.</param>
    /// <returns>Whether it is.</returns>
    internal static unsafe bool SkippedByWorkers(Trigger* trigger) => BareGuard(trigger);

    // Whether a trigger of a table a statement writes is one of the guard's that holds no condition of its own.
    private static unsafe bool BareGuard(Trigger* trigger)
    {
        const int Row = 1 << 0;
        const int Before = 1 << 1;
        const int Instead = 1 << 6;
        if ((trigger->tgqual is not null && (nint)trigger->tgqual != s_conditionMark) || (trigger->tgtype & (Row | Before | Instead)) != Row ||
            *trigger->tgname != (sbyte)'w')
        {
            return false;
        }

        ReadOnlySpan<byte> name = MemoryMarshal.CreateReadOnlySpanFromNullTerminated((byte*)trigger->tgname);
        return (name.SequenceEqual(GuardNameBytes) || name.SequenceEqual(UpdateGuardNameBytes)) && HooksGuard(trigger->tgfoid);
    }

    // Finds, among a table's triggers, the guard's trigger for updated rows that holds no condition, where the table
    // has none for its new rows, as a partitioned table and its partitions have: a statement then checks the new
    // rows itself, by what that trigger's arguments say the guard checks. Null for any other table.
    private static unsafe Trigger* RoutedGuard(TriggerDesc* triggers)
    {
        Trigger* found = null;
        for (int i = 0; triggers is not null && i < triggers->numtriggers; i++)
        {
            Trigger* trigger = &triggers->triggers[i];
            if (!BareGuard(trigger))
            {
                continue;
            }

            if ((trigger->tgtype & TriggerTypeInsert) != 0)
            {
                return null;
            }

            found = trigger;
        }

        return found;
    }

    // Builds the check a table's new rows pass before they're written, as PostgreSQL runs a row-level security
    // policy's: true for a row the guard passes, and the guard raises its own error for any other. The columns are
    // those of a range table entry.
    private static unsafe WithCheckOption* RowCheck(Trigger* guard, RelationData* told, RelationData* table, uint entry)
    {
        List<(string Name, string Type, List<string> Sequences)> named = CheckedByName(guard, told);
        if (named.Count == 0)
        {
            return null;
        }

        Node* condition = AnalyzedCondition(table, "(" + SequenceRanges.InsertCondition("worf.sequence_range_check", named) + ") IS NOT TRUE");
        NativeMethods.ChangeVarNodes(condition, 2, (int)entry, 0);
        WithCheckOption* check = (WithCheckOption*)NativeMethods.palloc0((nuint)sizeof(WithCheckOption));
        check->type = NodeTag.T_WithCheckOption;
        check->kind = WCOKind.WCO_RLS_INSERT_CHECK;
        check->relname = NativeMethods.pstrdup((sbyte*)&table->rd_rel->relname);
        check->qual = (Node*)NativeMethods.make_ands_implicit(NativeMethods.expression_planner((Expr*)condition));
        return check;
    }

    /// <summary>
    /// Reads what a guard trigger checks from its arguments: the columns, by their numbers in the table, with whether
    /// each is checked as a numeric, and the sequences each is checked under. The arguments number the columns as the
    /// table the trigger was created on does, which a partition that took the trigger from it may number otherwise,
    /// so a partition's are found by name. A name that an argument gives in place of a sequence, which only a trigger
    /// with a condition checks, is left out.
    /// </summary>
    private static unsafe List<(short Column, bool Numeric, uint[] Sequences)> GuardedColumns(uint relid, List<string> arguments)
    {
        List<(short Column, bool Numeric, uint[] Sequences)> columns = [];
        RelationData* table = NativeMethods.relation_open(relid, (int)PgLockMode.None);
        RelationData* root = null;
        try
        {
            if (table->rd_rel->relispartition)
            {
                List* ancestors = NativeMethods.get_partition_ancestors(relid);
                if (ancestors is not null && ancestors->length > 0)
                {
                    root = NativeMethods.relation_open(ancestors->elements[ancestors->length - 1].oid_value, (int)PgLockMode.AccessShare);
                }
            }

            foreach (string argument in arguments)
            {
                int colon = argument.IndexOf(':', StringComparison.Ordinal);
                if (colon <= 0 || !short.TryParse(argument.AsSpan(0, colon), NumberStyles.None, CultureInfo.InvariantCulture, out short column))
                {
                    continue;
                }

                if (root is not null)
                {
                    if (column > root->rd_att->natts)
                    {
                        continue;
                    }

                    column = NativeMethods.get_attnum(relid, (sbyte*)&NativeMethods.TupleDescAttr(root->rd_att, column - 1)->attname);
                }

                if (column <= 0 || column > table->rd_att->natts)
                {
                    continue;
                }

                List<uint> sequences = [.. argument[(colon + 1)..].Split(',')
                    .Select(static named => uint.TryParse(named, NumberStyles.None, CultureInfo.InvariantCulture, out uint seqoid) ? seqoid : (uint?)null)
                    .OfType<uint>()];
                if (sequences.Count > 0)
                {
                    columns.Add((column, NativeMethods.TupleDescAttr(table->rd_att, column - 1)->atttypid == (uint)PgBuiltInOid.NumericOid, [.. sequences]));
                }
            }
        }
        finally
        {
            if (root is not null)
            {
                NativeMethods.relation_close(root, (int)PgLockMode.AccessShare);
            }

            NativeMethods.relation_close(table, (int)PgLockMode.None);
        }

        return columns;
    }

    // What a guard trigger's arguments say it checks of a table's rows, with the columns by quoted name, the type
    // each value is checked as, and its sequences as the check takes them.
    private static unsafe List<(string Name, string Type, List<string> Sequences)> CheckedByName(Trigger* trigger, RelationData* table)
    {
        List<string> arguments = [];
        for (int i = 0; i < trigger->tgnargs; i++)
        {
            arguments.Add(Worf.FromServer(trigger->tgargs[i]));
        }

        return [.. GuardedColumns(table->rd_id, arguments).Select(column => (
            Worf.QuoteIdentifier(Worf.FromServer((sbyte*)&NativeMethods.TupleDescAttr(table->rd_att, column.Column - 1)->attname)), column.Numeric ? "numeric" : "bigint",
            column.Sequences.Select(seqoid => Worf.QuoteLiteral(seqoid.ToString(CultureInfo.InvariantCulture)) + "::regclass").ToList()))];
    }

    // Analyzes a condition on a table's rows, given as text that names a row's old and new versions, as PostgreSQL
    // analyzes a trigger's: the old version's columns come out as those of range table entry 1 and the new one's as
    // those of entry 2. The text is parsed as worf's own, which runs nothing of the user's.
    private static unsafe Node* AnalyzedCondition(RelationData* table, string text)
        => (Node*)Elevated.Run(() => Unnamed(() =>
        {
            Node* raw = Parsed("SELECT " + text);
            ParseState* pstate = NativeMethods.make_parsestate(null);
            try
            {
                foreach (string alias in s_rowAliases)
                {
                    ParseNamespaceItem* item = NativeMethods.addRangeTableEntryForRelation(pstate, table, (int)PgLockMode.AccessShare,
                        NativeMethods.makeAlias(Worf.PStrDup(alias), null), false, false);
                    NativeMethods.addNSItemToQuery(pstate, item, false, true, true);
                }

                Node* condition = NativeMethods.transformWhereClause(pstate, raw, ParseExprKind.EXPR_KIND_TRIGGER_WHEN, Worf.PStrDup("WHEN"));
                NativeMethods.assign_expr_collations(pstate, condition);
                return (nint)condition;
            }
            finally
            {
                NativeMethods.free_parsestate(pstate);
            }
        }));

    // Builds the condition the guard checks a table's new rows under, or its updated rows, from what a guard trigger's
    // arguments say it checks, as PostgreSQL builds a trigger's own: the row's old and new versions are the inner and
    // the outer row.
    private static unsafe nint BuildCondition(Trigger* trigger, uint relid, bool update, MemoryContextData* memory)
    {
        RelationData* table = NativeMethods.relation_open(relid, (int)PgLockMode.None);
        MemoryContextData* outer = Worf.MemoryContextSwitchTo(memory);
        try
        {
            List<(string Name, string Type, List<string> Sequences)> named = CheckedByName(trigger, table);
            if (named.Count == 0)
            {
                return 0;
            }

            Node* condition = AnalyzedCondition(table, update ? SequenceRanges.UpdateCondition(named) : SequenceRanges.InsertCondition("worf.sequence_range_check", named));
            NativeMethods.ChangeVarNodes(condition, 1, OldRowVar, 0);
            NativeMethods.ChangeVarNodes(condition, 2, NewRowVar, 0);
            return (nint)NativeMethods.ExecInitQual(NativeMethods.make_ands_implicit(NativeMethods.expression_planner((Expr*)condition)), null);
        }
        finally
        {
            _ = Worf.MemoryContextSwitchTo(outer);
            NativeMethods.relation_close(table, (int)PgLockMode.None);
        }
    }

    /// <summary>
    /// Has the partitions a statement's rows reach through a partitioned table check the rows as they come, where the
    /// table's guard triggers hold no condition and its rows don't take their defaults: a partition opens as its
    /// first row comes, with triggers that don't fire in this session, so the statement gives each partition it opens
    /// a check of the new row to run before it writes it, as PostgreSQL has a partition take the checks a row-level
    /// security policy puts on the table the statement names. The check never fails as a policy's does: it is true
    /// for a row the guard passes, and the guard raises its own error for any other.
    /// </summary>
    private static unsafe void CheckRoutedRows(PlanState* node, EState* state)
    {
        if (node is null)
        {
            return;
        }

        if (node->type != NodeTag.T_ModifyTableState)
        {
            CheckRoutedRows(node->lefttree, state);
            CheckRoutedRows(node->righttree, state);
            return;
        }

        ModifyTableState* modify = (ModifyTableState*)node;
        ModifyTable* plan = (ModifyTable*)node->plan;
        ResultRelInfo* root = modify->rootResultRelInfo;
        if (modify->mt_nrels == 0 || root is null || root->ri_RelationDesc->rd_rel->relkind != (sbyte)'p' || root->ri_TrigDesc is null ||
            plan->operation is not (CmdType.CMD_INSERT or CmdType.CMD_MERGE))
        {
            return;
        }

        Trigger* guard = RoutedGuard(root->ri_TrigDesc);
        if (guard is null)
        {
            return;
        }

        // The plan may be one PostgreSQL keeps for other runs, so the statement's own copy of its top holds the check.
        ModifyTable* own = (ModifyTable*)NativeMethods.MemoryContextAlloc(state->es_query_cxt, (nuint)sizeof(ModifyTable));
        *own = *plan;
        node->plan = (Plan*)own;
        CheckRoutedUpdates(modify, own, root, guard, state);

        // The check is written for the first table the statement writes, as PostgreSQL has every partition's made
        // from that one's, with the columns as that table numbers them.
        ResultRelInfo* first = &modify->resultRelInfo[0];
        MemoryContextData* outer = Worf.MemoryContextSwitchTo(state->es_query_cxt);
        try
        {
            WithCheckOption* check = RowCheck(guard, root->ri_RelationDesc, first->ri_RelationDesc, first->ri_RangeTableIndex);
            if (check is null)
            {
                return;
            }

            // A row a MERGE's UPDATE moves to a partition that opens for it is checked as a new row too, as the
            // partition's own trigger checked it where the MERGE has an INSERT action: PostgreSQL runs the checks
            // of an update for it, and no row is updated in place in a partition the MERGE's plan left out.
            WithCheckOption* moved = null;
            if (plan->operation == CmdType.CMD_MERGE)
            {
                moved = (WithCheckOption*)NativeMethods.palloc((nuint)sizeof(WithCheckOption));
                *moved = *check;
                moved->kind = WCOKind.WCO_RLS_UPDATE_CHECK;
            }

            List* lists = null;
            int tables = plan->resultRelations is null ? 1 : plan->resultRelations->length;
            for (int i = 0; i < tables; i++)
            {
                List* given = plan->withCheckOptionLists is not null && i < plan->withCheckOptionLists->length
                    ? NativeMethods.list_copy((List*)plan->withCheckOptionLists->elements[i].ptr_value)
                    : null;
                given = NativeMethods.lappend(given, check);
                lists = NativeMethods.lappend(lists, moved is null ? given : NativeMethods.lappend(given, moved));
            }

            own->withCheckOptionLists = lists;
        }
        finally
        {
            _ = Worf.MemoryContextSwitchTo(outer);
        }
    }

    // Has the values an INSERT's ON CONFLICT DO UPDATE sets the guarded columns of a partitioned table to pass
    // through the guard's check, with the value the row held, in the statement's own copy of its plan's top, which
    // each partition's update of a row it conflicts with is made from.
    private static unsafe void CheckRoutedUpdates(ModifyTableState* modify, ModifyTable* own, ResultRelInfo* root, Trigger* guard, EState* state)
    {
        const uint TextOid = 25;
        const uint DefaultCollation = 100;
        const uint Int2Oid = 21;
        const uint Int4Oid = 23;
        const uint BigintFromSmallint = 754;
        const uint BigintFromInteger = 481;
        const uint SmallintFromBigint = 714;
        const uint IntegerFromBigint = 480;
        if (own->onConflictAction != OnConflictAction.ONCONFLICT_UPDATE || own->onConflictSet is null || own->onConflictCols is null)
        {
            return;
        }

        List<string> arguments = [];
        for (int i = 0; i < guard->tgnargs; i++)
        {
            arguments.Add(Worf.FromServer(guard->tgargs[i]));
        }

        MemoryContextData* outer = Worf.MemoryContextSwitchTo(state->es_query_cxt);
        try
        {
            List* set = null;
            foreach ((short column, bool numeric, uint[] sequences) in GuardedColumns(root->ri_RelationDesc->rd_id, arguments))
            {
                for (int i = 0; i < own->onConflictCols->length && i < own->onConflictSet->length; i++)
                {
                    if (own->onConflictCols->elements[i].int_value != column)
                    {
                        continue;
                    }

                    set = set is null ? NativeMethods.list_copy(own->onConflictSet) : set;
                    FormData_pg_attribute* attribute = NativeMethods.TupleDescAttr(root->ri_RelationDesc->rd_att, column - 1);
                    uint type = attribute->atttypid;
                    uint wide = numeric ? (uint)PgBuiltInOid.NumericOid : BigintOid;
                    uint[] signature = [TextOid, wide, wide];
                    List* name = NativeMethods.lappend(null, NativeMethods.makeString(Worf.PStrDup(Worf.ExtensionName)));
                    name = NativeMethods.lappend(name, NativeMethods.makeString(Worf.PStrDup("sequence_range_checked")));
                    uint function = Elevated.Run(() =>
                    {
                        fixed (uint* types = signature)
                        {
                            return NativeMethods.LookupFuncName(name, 3, types, false);
                        }
                    });

                    Expr* Widened(Expr* value)
                        => type is Int2Oid or Int4Oid
                            ? (Expr*)NativeMethods.makeFuncExpr(type == Int2Oid ? BigintFromSmallint : BigintFromInteger, BigintOid,
                                NativeMethods.lappend(null, value), 0, 0, CoercionForm.COERCE_IMPLICIT_CAST)
                            : value;

                    TargetEntry* entry = NativeMethods.flatCopyTargetEntry((TargetEntry*)set->elements[i].ptr_value);
                    Var* was = NativeMethods.makeVar((int)root->ri_RangeTableIndex, column, type, attribute->atttypmod, attribute->attcollation, 0);
                    Const* under = NativeMethods.makeConst(TextOid, -1, DefaultCollation, -1,
                        (ulong)NativeMethods.cstring_to_text(Worf.PStrDup(string.Join(',', sequences))), false, false);
                    List* given = NativeMethods.lappend(NativeMethods.lappend(NativeMethods.lappend(null, under), Widened((Expr*)was)), Widened(entry->expr));
                    Expr* passed = (Expr*)NativeMethods.makeFuncExpr(function, wide, given, 0, DefaultCollation, CoercionForm.COERCE_EXPLICIT_CALL);
                    entry->expr = type is Int2Oid or Int4Oid
                        ? (Expr*)NativeMethods.makeFuncExpr(type == Int2Oid ? SmallintFromBigint : IntegerFromBigint, type,
                            NativeMethods.lappend(null, passed), 0, 0, CoercionForm.COERCE_IMPLICIT_CAST)
                        : passed;
                    set->elements[i].ptr_value = entry;
                }
            }

            if (set is not null)
            {
                own->onConflictSet = set;

                // A partition whose rows are laid out as the partitioned table's takes what the latter built to set
                // the columns with, so that is built again from the values that pass through the check.
                if (root->ri_onConflict is not null && root->ri_onConflict->oc_ProjInfo is not null && modify->ps.ps_ExprContext is not null)
                {
                    root->ri_onConflict->oc_ProjInfo = NativeMethods.ExecBuildUpdateProjection(set, true, own->onConflictCols,
                        root->ri_RelationDesc->rd_att, modify->ps.ps_ExprContext, root->ri_onConflict->oc_ProjSlot, &modify->ps);
                }
            }
        }
        finally
        {
            _ = Worf.MemoryContextSwitchTo(outer);
        }
    }

    // The expression's state that works out the rows a statement inserts, or the default a COPY's rows take, with
    // where it leaves the value of the column the guard checks, while the statement's tables take their conditions:
    // the condition of a table whose triggers may change a row is then one that first sets the row's value against
    // the value noted from there.
    private static (nint State, nint Value, nint Null) s_noting;

    // The condition that first sets a new row's value against the value its default drew, and checks only a value
    // that differs, with a step added to the expression that works the row out, which notes the value drawn where
    // the condition reads it. Zero where the expression takes no step, and the row is then checked as any other.
    private static unsafe nint NotingCondition(Trigger* trigger, uint relid)
    {
        const uint Int2Unequal = 145;
        const uint Int4Unequal = 144;
        const uint Int8Unequal = 468;
        ExprState* works = (ExprState*)s_noting.State;
        if (!ReadStepCodes() || (works->flags & InterpreterInitialized) == 0 || (nint)works->evalfunc_private != s_interpreter ||
            works->steps_len < 1 || works->steps_len + 1 > works->steps_alloc)
        {
            return 0;
        }

        _ = Condition(trigger, relid, false);
        if (!s_conditions.TryGetValue(relid, out WorfGuardConditions? conditions))
        {
            return 0;
        }

        if (conditions.Noting == 0)
        {
            RelationData* table = NativeMethods.relation_open(relid, (int)PgLockMode.None);
            MemoryContextData* outer = Worf.MemoryContextSwitchTo((MemoryContextData*)conditions.Context);
            try
            {
                List<(string Name, string Type, List<string> Sequences)> named = CheckedByName(trigger, table);
                if (named.Count != 1)
                {
                    return 0;
                }

                // The value drawn is compared as the column's own type, in place of a constant of that type.
                string type = "int8";
                for (int i = 0; i < table->rd_att->natts; i++)
                {
                    FormData_pg_attribute* attribute = NativeMethods.TupleDescAttr(table->rd_att, i);
                    if (!attribute->attisdropped && Worf.QuoteIdentifier(Worf.FromServer((sbyte*)&attribute->attname)) == named[0].Name)
                    {
                        type = attribute->atttypid == 21 ? "int2" : attribute->atttypid == 23 ? "int4" : "int8";
                    }
                }

                Node* condition = AnalyzedCondition(table,
                    $"(NEW.{named[0].Name} <> '0'::pg_catalog.{type}) AND ({SequenceRanges.InsertCondition("worf.sequence_range_check", named)})");
                NativeMethods.ChangeVarNodes(condition, 1, OldRowVar, 0);
                NativeMethods.ChangeVarNodes(condition, 2, NewRowVar, 0);
                ExprState* built = NativeMethods.ExecInitQual(NativeMethods.make_ands_implicit(NativeMethods.expression_planner((Expr*)condition)), null);

                // The comparison's second argument, a constant as the condition was built, is where the value drawn goes.
                for (int i = 0; i < built->steps_len && conditions.Drawn == 0; i++)
                {
                    ExprEvalStep* step = &built->steps[i];
                    if (CallsFunction(StepKind(built, step)) && step->d.func.nargs == 2 && step->d.func.finfo->fn_oid is Int2Unequal or Int4Unequal or Int8Unequal)
                    {
                        conditions.Drawn = (nint)((byte*)step->d.func.fcinfo_data + sizeof(FunctionCallInfoBaseData) + sizeof(NullableDatum));
                    }
                }

                conditions.Noting = conditions.Drawn == 0 ? -1 : (nint)built;
            }
            finally
            {
                _ = Worf.MemoryContextSwitchTo(outer);
                NativeMethods.relation_close(table, (int)PgLockMode.None);
            }
        }

        if (conditions.Noting <= 0)
        {
            return 0;
        }

        // The step goes last, ahead of the one that ends the expression, where the row's values all stand.
        NullableDatum* drawn = (NullableDatum*)conditions.Drawn;
        ExprEvalStep* steps = works->steps;
        steps[works->steps_len] = steps[works->steps_len - 1];
        ExprEvalStep* notes = &steps[works->steps_len - 1];
        *notes = default;
        notes->opcode = s_valueCode;
        notes->resvalue = &drawn->value;
        notes->resnull = &drawn->isnull;
        notes->d.casetest.value = (ulong*)s_noting.Value;
        notes->d.casetest.isnull = (bool*)s_noting.Null;
        works->steps_len++;
        return conditions.Noting;
    }

    // The condition a statement has a guard trigger that holds none run for a table's rows, built on its first use
    // since the table last changed.
    private static unsafe nint Condition(Trigger* trigger, uint relid, bool update)
    {
        WatchTables();
        if (s_conditions.TryGetValue(relid, out WorfGuardConditions? conditions) && conditions.Stale)
        {
            // The statements that run the conditions built before the change may run them on, to their transaction's end.
            s_retiredConditions.Add(conditions.Context);
            if (!s_retiringConditions)
            {
                s_retiringConditions = true;
                _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, FreeRetiredConditions);
                _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, FreeRetiredConditions);
            }

            _ = s_conditions.Remove(relid);
            conditions = null;
        }

        if (conditions is null)
        {
            fixed (byte* name = "worf guard conditions"u8)
            {
                conditions = new() { Context = (nint)NativeMethods.AllocSetContextCreateInternal(NativeGlobals.TopMemoryContext, (sbyte*)name, 0, 1024, 8192) };
            }

            s_conditions.Add(relid, conditions);
        }

        if ((update ? conditions.Update : conditions.Insert) == 0)
        {
            nint built = BuildCondition(trigger, relid, update, (MemoryContextData*)conditions.Context);
            if (update)
            {
                conditions.Update = built;
            }
            else
            {
                conditions.Insert = built;
            }
        }

        return update ? conditions.Update : conditions.Insert;
    }

    private static unsafe void FreeRetiredConditions()
    {
        s_retiringConditions = false;
        foreach (nint context in s_retiredConditions)
        {
            NativeMethods.MemoryContextDelete((MemoryContextData*)context);
        }

        s_retiredConditions.Clear();
    }

    /// <summary>
    /// Has the guard triggers that hold no condition of their own, in the copies a statement keeps of its tables'
    /// triggers, do what the statement needs: the rows of a statement that takes the guarded columns' values from
    /// the columns' defaults, and those worf's own apply and sync workers write, skip them; the new rows of any other
    /// statement that inserts, and the rows of one that updates, run the condition the guard built for the table, as
    /// they would a trigger's own. A trigger left as it is fires where its table has it fire, and its function checks.
    /// </summary>
    /// <param name="written">A table the statement writes, as it opened it.</param>
    /// <param name="takes">Whether the statement's rows take the guarded columns' values from their defaults.</param>
    /// <param name="inserts">Whether the statement may insert rows.</param>
    /// <param name="updates">Whether the statement may update rows.</param>
    /// <param name="memory">The statement's memory.</param>
    private static unsafe void AttachGuards(ResultRelInfo* written, bool takes, bool inserts, bool updates, MemoryContextData* memory)
    {
        const int Row = 1 << 0;
        const int Before = 1 << 1;
        const int Instead = 1 << 6;
        const sbyte Disabled = (sbyte)'D';
        const sbyte Always = (sbyte)'A';
        TriggerDesc* triggers = written->ri_TrigDesc;
        if (triggers is null || written->ri_TrigWhenExprs is null)
        {
            return;
        }

        bool guards = false;
        for (int i = 0; i < triggers->numtriggers && !guards; i++)
        {
            guards = BareGuard(&triggers->triggers[i]);
        }

        if (!guards)
        {
            return;
        }

        if (s_conditionMark == 0)
        {
            MemoryContextData* outer = Worf.MemoryContextSwitchTo(NativeGlobals.TopMemoryContext);
            s_conditionMark = (nint)Worf.PStrDup("<>");
            _ = Worf.MemoryContextSwitchTo(outer);
        }

        bool worker = Worker.MyWorfWorker >= 0 && Worker.MyWorker.WorkerType is WorfWorkerType.Apply or WorfWorkerType.Sync;
        uint relid = written->ri_RelationDesc->rd_id;
        bool fires = false;
        for (int i = 0; i < triggers->numtriggers; i++)
        {
            Trigger* trigger = &triggers->triggers[i];
            bool insert = (trigger->tgtype & (Row | Before | TriggerTypeInsert | Instead)) == (Row | TriggerTypeInsert);
            if (BareGuard(trigger))
            {
                bool update = !insert;
                if (worker || (insert && takes))
                {
                    trigger->tgenabled = Disabled;
                }
                else if (insert ? inserts : updates)
                {
                    nint condition = insert && s_noting.State != 0 ? NotingCondition(trigger, relid) : 0;
                    if (condition == 0)
                    {
                        condition = Condition(trigger, relid, update);
                    }

                    if (condition != 0)
                    {
                        trigger->tgqual = (sbyte*)s_conditionMark;
                        written->ri_TrigWhenExprs[i] = (ExprState*)condition;
                        trigger->tgenabled = Always;
                    }
                }
            }

            fires |= insert && trigger->tgenabled != Disabled;
        }

        // Where no trigger fires after a row's insert, PostgreSQL then looks at none for it.
        if (!fires)
        {
            triggers->trig_insert_after_row = false;
        }

        // A partition a statement writes itself has no trigger for its new rows either, so the statement's copy of
        // its triggers takes one on, with the condition the guard built, which fires for the rows the statement
        // inserts there and those it moves there, as a partition's own trigger would.
        Trigger* routed = worker || takes || !inserts || memory is null || !written->ri_RelationDesc->rd_rel->relispartition ? null : RoutedGuard(triggers);
        nint check = routed is null ? 0 : Condition(routed, relid, false);
        if (check != 0)
        {
            int count = triggers->numtriggers;
            TriggerDesc* own = (TriggerDesc*)NativeMethods.MemoryContextAlloc(memory, (nuint)sizeof(TriggerDesc));
            *own = *triggers;
            own->triggers = (Trigger*)NativeMethods.MemoryContextAllocZero(memory, (nuint)((count + 1) * sizeof(Trigger)));
            Buffer.MemoryCopy(triggers->triggers, own->triggers, (long)(count + 1) * sizeof(Trigger), (long)count * sizeof(Trigger));
            Trigger* added = &own->triggers[count];
            added->tgoid = uint.MaxValue - 1;
            added->tgname = (sbyte*)GuardNameText();
            added->tgfoid = routed->tgfoid;
            added->tgtype = Row | TriggerTypeInsert;
            added->tgenabled = Always;
            added->tgisinternal = true;
            added->tgnargs = routed->tgnargs;
            added->tgargs = routed->tgargs;
            added->tgqual = (sbyte*)s_conditionMark;
            own->numtriggers = count + 1;
            own->trig_insert_after_row = true;
            FmgrInfo* functions = (FmgrInfo*)NativeMethods.MemoryContextAllocZero(memory, (nuint)((count + 1) * sizeof(FmgrInfo)));
            Buffer.MemoryCopy(written->ri_TrigFunctions, functions, (long)(count + 1) * sizeof(FmgrInfo), (long)count * sizeof(FmgrInfo));
            ExprState** conditions = (ExprState**)NativeMethods.MemoryContextAllocZero(memory, (nuint)((count + 1) * sizeof(nint)));
            Buffer.MemoryCopy(written->ri_TrigWhenExprs, conditions, (long)(count + 1) * sizeof(nint), (long)count * sizeof(nint));
            conditions[count] = (ExprState*)check;
            // What EXPLAIN ANALYZE notes of each trigger, which PostgreSQL 19 keeps in a type of its own.
            if (written->ri_TrigInstrument is not null)
            {
#if ANKUS_PG19
                TriggerInstrumentation* timings = (TriggerInstrumentation*)NativeMethods.MemoryContextAllocZero(memory, (nuint)((count + 1) * sizeof(TriggerInstrumentation)));
                Buffer.MemoryCopy(written->ri_TrigInstrument, timings, (long)(count + 1) * sizeof(TriggerInstrumentation), (long)count * sizeof(TriggerInstrumentation));
#else
                Instrumentation* timings = (Instrumentation*)NativeMethods.MemoryContextAllocZero(memory, (nuint)((count + 1) * sizeof(Instrumentation)));
                Buffer.MemoryCopy(written->ri_TrigInstrument, timings, (long)(count + 1) * sizeof(Instrumentation), (long)count * sizeof(Instrumentation));
#endif
                written->ri_TrigInstrument = timings;
            }

            written->ri_TrigDesc = own;
            written->ri_TrigFunctions = functions;
            written->ri_TrigWhenExprs = conditions;
        }
    }

    // The guard trigger's name as PostgreSQL holds a trigger's, kept for the backend.
    private static nint s_guardNameText;

    private static unsafe nint GuardNameText()
    {
        if (s_guardNameText == 0)
        {
            MemoryContextData* outer = Worf.MemoryContextSwitchTo(NativeGlobals.TopMemoryContext);
            s_guardNameText = (nint)Worf.PStrDup(SequenceRanges.GuardName);
            _ = Worf.MemoryContextSwitchTo(outer);
        }

        return s_guardNameText;
    }

    // Has the guard triggers of every table a statement has opened to write do what the statement needs.
    private static unsafe void AttachGuards(EState* state, bool takes, bool inserts, bool updates)
    {
        List* opened = state is null ? null : state->es_opened_result_relations;
        for (int i = 0; opened is not null && i < opened->length; i++)
        {
            AttachGuards((ResultRelInfo*)opened->elements[i].ptr_value, takes, inserts, updates, state->es_query_cxt);
        }
    }

    // Notes where a gated sequence is as a statement that holds its gate starts, or one of its rows is checked, so that
    // the next row's check sees how many values the row drew.
    private static void MarkRow(uint seqoid)
    {
        if (Position(seqoid) is long position)
        {
            s_rowMarks[seqoid] = position;
        }
        else
        {
            _ = s_rowMarks.Remove(seqoid);
        }
    }

    // Counts the values a row of a statement that holds a sequence's gate drew since the last mark, within the active
    // block, other sessions' draws meanwhile too, toward how many the statement's later rows, and the session's next such
    // statement, count each row to draw, up to as many as a block holds past its last twentieth, so that a trigger that
    // draws more than its source shows, in a loop or through a function it calls, waits for the switch rather than run
    // the block out. Rows whose source shows every draw aren't counted so, as other sessions' draws would count as
    // theirs.
    private static void SeeRowDraws(uint seqoid)
    {
        if (!s_gates.ContainsKey(seqoid))
        {
            return;
        }

        long? position = Position(seqoid);
        if (s_unseenRowDraws.Contains(seqoid) && s_rowMarks.TryGetValue(seqoid, out long mark) && position is long now &&
            s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence) &&
            sequence.Blocks.FindIndex(block => block.State == "active") is int index and >= 0 &&
            sequence.Blocks[index] is (long lo, long hi, _) && mark >= lo && mark <= hi && now >= lo && now <= hi)
        {
            decimal step = Step(seqoid);
            long drawn = (long)Math.Min(Math.Abs((decimal)now - mark) / step, RowDrawsAtMost(seqoid));
            if (drawn > s_gatedRowDraws.GetValueOrDefault(seqoid, 1))
            {
                s_gatedRowDraws[seqoid] = drawn;
            }

            s_seenRowDraws[seqoid] = s_seenThisStatement.Add(seqoid) ? drawn : Math.Max(s_seenRowDraws.GetValueOrDefault(seqoid), drawn);
        }

        if (position is long at)
        {
            s_rowMarks[seqoid] = at;
        }
    }

    // The calls of nextval the CHECK constraints, trigger conditions and trigger functions of tables and their partitions
    // make for a row, as their text names them, and whether the trigger functions may make more than their text shows,
    // as one that loops, runs a statement it builds or calls a function of a user's does, once until a table, a
    // constraint or a function changes.
    private static (int Calls, bool Unseen) WrittenCalls(List<uint> written)
    {
        string key = string.Join(',', written.Order());
        if (!s_writtenCalls.TryGetValue(key, out (int Calls, bool Unseen) counted))
        {
            WatchTables();
            long[] relids = [.. written.Select(table => (long)table)];
            string[] sources = [.. Elevated.Run(() => Worf.Read("""
                WITH t (relid) AS (
                    SELECT r.relid::pg_catalog.oid FROM pg_catalog.unnest($1::pg_catalog.int8[]) r (relid)
                    UNION ALL
                    SELECT p.relid FROM pg_catalog.unnest($1::pg_catalog.int8[]) r (relid)
                        CROSS JOIN LATERAL pg_catalog.pg_partition_tree(r.relid::pg_catalog.oid) p WHERE p.level > 0)
                SELECT f.prosrc FROM t JOIN pg_catalog.pg_trigger g ON g.tgrelid = t.relid
                    JOIN pg_catalog.pg_proc f ON f.oid = g.tgfoid JOIN pg_catalog.pg_language l ON l.oid = f.prolang
                WHERE l.lanname NOT IN ('c', 'internal')
                """, 0, SpiParameter.Create(relids))).Select(row => row.Get<string?>(0) ?? "")];
            string[] called = [.. sources.SelectMany(source => CalledName().Matches(source).Select(call => call.Groups[1].Value.ToLowerInvariant())).Distinct()];
            bool unseen = sources.Any(source => ExecuteWord().IsMatch(source) || (NextvalCalls(source) > 0 && (LoopWord().IsMatch(source) || DrawsOverRows().IsMatch(source)))) ||
                (called.Length > 0 && Elevated.Run(() => Worf.ReadScalar<bool>("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                        WHERE p.proname = ANY ($1::pg_catalog.text[]) AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'worf'))
                    """, SpiParameter.Create(called))));
            counted = (Draws(WrittenChecks(relids)).Calls + (int)Math.Min(sources.Sum(source => NextvalCalls(source) * Repeats(source)), int.MaxValue / 2), unseen);
            s_writtenCalls[key] = counted;
        }

        return counted;
    }

    // How many times a function's source runs its calls at most, as far as its text shows: the most rows a loop over
    // constant bounds, or a generate_series of constants, gives, or once.
    private static long Repeats(string source)
    {
        long repeats = 1;
        foreach (Match bounds in ConstantRange().Matches(source))
        {
            int first = bounds.Groups[1].Success ? 1 : 3;
            if (long.TryParse(bounds.Groups[first].Value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long from) &&
                long.TryParse(bounds.Groups[first + 1].Value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long to))
            {
                repeats = Math.Max(repeats, (long)Math.Min(Math.Abs((decimal)to - from) + 1, int.MaxValue));
            }
        }

        return repeats;
    }

    // A loop over constant bounds, FOR i IN 1..12, or a generate_series of constants, generate_series(1, 12).
    [GeneratedRegex(@"\bin\s+(-?[0-9]+)\s*\.\.\s*(-?[0-9]+)|\bgenerate_series\s*\(\s*(-?[0-9]+)\s*,\s*(-?[0-9]+)\s*\)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex ConstantRange();

    // A call of nextval in a query over rows, SELECT nextval(...) FROM or PERFORM nextval(...) FROM, which draws once for
    // each of its rows, however many its text shows.
    [GeneratedRegex(@"nextval\s*\([^;]*?\bfrom\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline)]
    private static partial Regex DrawsOverRows();

    // A word of a function's source that runs a statement it builds, whose draws its text doesn't show.
    [GeneratedRegex(@"\bexecute\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex ExecuteWord();

    // A word of a function's source that loops, whose calls its text shows once each however often they run.
    [GeneratedRegex(@"\bloop\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex LoopWord();

    // A name a function's source calls, as a function of a user's that may draw.
    [GeneratedRegex(@"\b([A-Za-z_][A-Za-z0-9_$]*)\s*\(", RegexOptions.CultureInvariant)]
    private static partial Regex CalledName();

    // A sequence's step, its increment's size, at least 1, which the guard reads for each row, kept until the sequence
    // changes, as ALTER SEQUENCE changes it, rather than read from the catalog each time.
    private static decimal Step(uint seqoid)
    {
        if (!s_steps.TryGetValue(seqoid, out decimal step))
        {
            WatchTables();
            step = Math.Max(Math.Abs((decimal)SequenceRanges.GetInfo(seqoid).Increment), 1);
            s_steps[seqoid] = step;
        }

        return step;
    }

    // The draws a gated row whose triggers may draw unseen counts for before the session has seen such a row draw: half
    // the sequence's active block.
    private static long RowDrawsUnknown(uint seqoid) => (RowDrawsAtMost(seqoid) / 2) + 1;

    // The most draws a gated row counts for, as many as a block of the active one's size holds past its last twentieth,
    // as Short counts no more: a row that draws more runs into the next block however soon the switch comes.
    private static long RowDrawsAtMost(uint seqoid)
    {
        Reload();
        if (!s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence) ||
            sequence.Blocks.FindIndex(block => block.State == "active") is not (int index and >= 0))
        {
            return 0;
        }

        (long lo, long hi, _) = sequence.Blocks[index];
        decimal step = Step(seqoid);
        return (long)(((decimal)hi - lo) / step * 19 / 20);
    }

    // Watches the trigger functions a statement may call, with the sequences its triggers name for each, until it ends.
    private static void HookTriggers(nint statement, Dictionary<uint, HashSet<uint>> functions)
    {
        if (!s_hookingRegistered)
        {
            s_hookingRegistered = true;
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, ForgetHooking);
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, ForgetHooking);
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Prepare, ForgetHooking);
            _ = PgTransaction.RegisterSubtransactionCallback(PgSubtransactionEvent.Abort,
                static (aborted, _) => s_hookingStatements.RemoveAll(entry => entry.Subxact >= aborted.Value));
        }

        uint subxact;
        unsafe
        {
            subxact = NativeMethods.GetCurrentSubTransactionId();
        }

        s_hookingStatements.Add((statement, subxact, functions));
    }

    private static void ForgetHooking()
    {
        s_hookingStatements.Clear();
        s_hookingRegistered = false;
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
    /// statements that hold the gate, which may each draw a row's values before they check again, so that however many
    /// statements draw at once, they don't run the block out. The sequence's position is read under the lock a SELECT from it takes,
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
    /// <param name="seqoid">The sequence.</param>
    /// <param name="draws">How many values the statement's next row may draw, which the block must hold past its last
    /// twentieth, so that a row that draws more than that waits before it starts rather than run the block out; a
    /// statement that holds the sequence's gate counts at least the values its rows draw.</param>
    private static void WaitWhileShort(uint seqoid, long draws = 1)
    {
        s_shortAt = default;
        // A row of a statement that holds the gate may draw more, before the row is checked.
        draws = Math.Max(draws, s_gates.ContainsKey(seqoid) ? s_gatedRowDraws.GetValueOrDefault(seqoid, 1) : 1);
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
            s_shortAt = (seqoid, position);

            // A position past the block, in the next one, as a switch to it that rolled back leaves it, has run the block
            // out, while its bounds are still the block's, as has one at its last value.
            bool past = position is long p && (sequence.Ascending ? p > hi : p < lo);
            if (position is long q && (q < lo || q > hi) && !past)
            {
                return;
            }

            if (!past && !Short(seqoid, sequence.Ascending, lo, hi, position, () => ReadPosition(seqoid), draws))
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
    /// come, as long as for the next block at a block's end.
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

        List<(uint Seqoid, bool Unit)> held = LetGates();
        try
        {
            long deadline = Worker.GetCurrentTimestamp() + BlockWait(seqoid);
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

                if (now - checkedComing >= 200_000)
                {
                    checkedComing = now;
                    FailIfWorfDropWaits(seqoid);
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
    /// starts, which <see cref="InPlpgsql"/> finds a running function's state through. PL/pgSQL calls into worf through
    /// it only as <see cref="WatchStatements"/> has it, as each call costs far more than a statement that does little.
    /// </summary>
    internal static unsafe void WatchPlpgsql()
    {
        fixed (byte* name = "PLpgSQL_plugin\0"u8)
        {
            s_plpgsqlPlugin = NativeMethods.find_rendezvous_variable((sbyte*)name);
        }

        if (*s_plpgsqlPlugin is null)
        {
            s_plugin = (PLpgSQL_plugin*)NativeMemory.AllocZeroed((nuint)sizeof(PLpgSQL_plugin));
            *s_plpgsqlPlugin = s_plugin;
            s_ownsPlugin = true;
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

            // What the statement's start read of its plan's calls a moment ago serves its calls that start too.
            (HashSet<uint> Called, int Calls, bool Unnamed)? read = s_readDraws.Statement == statement ? s_readDraws.Draws : null;
            s_readDraws = default;
            s_starting.Add(new(read is { } known ? () => known : () => PlanDraws(statement, source, parameters), (nint)NativeGlobals.error_context_stack, false,
                source == 0 || parameters ? null : () => StatementText(source, location, length), planned: true,
                compiles: (planned->jitFlags & JitExpressions) != 0));
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
    /// <param name="started">Whether the plan started, rather than failed as it did.</param>
    internal static void StartedPlan(nint queryDesc, bool started)
    {
        s_drawsFound = 0;
        s_drawsBracketed.Clear();
        if (s_starting.Count > 0 && s_starting[^1].Looked > 0)
        {
            s_planStarted[queryDesc] = s_starting[^1].Looked;
        }

        bool identities = false;
        bool takes = false;
        short verifies = 0;
        if (s_takingDefaults.Count > 0 && s_takingDefaults.Remove(queryDesc, out (List<uint> Written, List<uint> Identities, bool Routes, short Verifies) taking) && started)
        {
            unsafe
            {
                QueryDesc* query = (QueryDesc*)queryDesc;
                PlanState* top = query->planstate;
                ModifyTableState* modify = top is not null && top->type == NodeTag.T_ModifyTableState ? (ModifyTableState*)top : null;
                if (modify is not null && s_starting.Count > 0 && HookIdentityDraws(query, s_starting[^1], taking.Identities))
                {
                    identities = taking.Identities.Count > 0;
                    verifies = taking.Verifies;
                    takes = verifies == 0;

                    // The step that notes the value a row drew, for a table whose triggers may change it, is one
                    // PostgreSQL's interpreter runs, so the expression that works the rows out goes back to the
                    // interpreter where PostgreSQL was to compile it, as an identity column's draw does, before
                    // its calls are found: PostgreSQL has built no code for the statement yet as its plan starts.
                    // Its calls, which PostgreSQL compiled to go through the fmgr hook, are the interpreter's again.
                    PlanState* rows = verifies == 0 ? null : top->lefttree;
                    if (rows is not null && rows->ps_ProjInfo is not null && (rows->ps_ProjInfo->pi_state.flags & InterpreterInitialized) == 0)
                    {
                        ExprState* works = &rows->ps_ProjInfo->pi_state;
                        NativeMethods.ExecReadyInterpretedExpr(works);
                        for (int i = 0; i < works->steps_len && s_starting[^1].Hooked > 0; i++)
                        {
                            s_starting[^1].Hooked -= CallsNextval(works, &works->steps[i]) ? 1 : 0;
                        }
                    }

                    for (int i = 0; takes && i < modify->mt_nrels; i++)
                    {
                        SkipGuard(&modify->resultRelInfo[i]);
                    }

                    if (taking.Routes)
                    {
                        RegisterEnd();
                        s_routing.Add((queryDesc, NativeMethods.GetCurrentSubTransactionId()));
                    }
                }
                else
                {
                    // An identity column whose draw worf didn't find leaves the statement to hold its gates and
                    // the guard's trigger to check its rows, as a statement that gives its values by hand does.
                    s_gatingAfterAll = true;
                    try
                    {
                        GateStatement(query, taking.Written);
                    }
                    finally
                    {
                        s_gatingAfterAll = false;
                    }
                }
            }
        }

        // A statement that draws from a managed sequence without its switch gate, as a SELECT of nextval does, has
        // each of its calls go through the guard, so that one statement that draws more values than the block has left
        // waits for the switch, as separate statements do. Where its calls can't all be found, it counts down for
        // its sequences' checks before each row its plan's nodes produce instead: each row counts for as many draws
        // as the statement has calls of nextval, as a row may call each of them once.
        if (s_starting.Count > 0 && s_starting[^1].Switched.Count > 0 && !s_starting[^1].Explained)
        {
            unsafe
            {
                QueryDesc* query = (QueryDesc*)queryDesc;
                List<nint> nodes = [];

                // A statement that started no call of nextval, but whose identity columns' draws run as calls that go
                // through the guard, has no row to count.
                if (!GuardPlan(query, s_starting[^1]) && (s_starting[^1].Looked > 0 || !identities))
                {
                    CountRows(query->planstate, nodes, Math.Max(s_starting[^1].Draws.Calls, 1), named: s_starting[^1].Draws.Unnamed ? null : s_starting[^1].Draws.Called);
                }

                if (nodes.Count > 0)
                {
                    RegisterCounted();
                    s_countedNodes[queryDesc] = (NativeMethods.GetCurrentSubTransactionId(), nodes);
                }
            }
        }

        // The guard triggers that hold no condition of their own run the one the guard built for their table, for
        // the rows of a statement that writes values to check, and are skipped for the rows that take their defaults.
        if (started)
        {
            unsafe
            {
                QueryDesc* query = (QueryDesc*)queryDesc;
                SettleGuards(query, takes, verifies);
                if ((takes || verifies != 0) && s_starting.Count > 0)
                {
                    SettleInsert(query, s_starting[^1], verifies);
                }
            }
        }

        s_drawsFound = 0;
        s_drawsBracketed.Clear();
        CheckGatedRows(queryDesc);
        for (int i = 0; i < s_ungatedDrawers.Count; i++)
        {
            if (s_ungatedDrawers[i] is { Statement: 0 } drawer && drawer.Depth == s_starting.Count)
            {
                s_ungatedDrawers[i] = drawer with { Statement = queryDesc };
            }
        }

        StartedExpressions();
    }

    // Counts a statement no more among those drawing from its sequences, as it ends, or once it has run to its end: the
    // draws it could still take without a check go back to the other statements, and the transaction's next draw checks
    // first.
    private static void EndDrawing(nint queryDesc)
    {
        for (int i = s_ungatedDrawers.Count - 1; i >= 0; i--)
        {
            if (s_ungatedDrawers[i].Statement == queryDesc)
            {
                uint seqoid = s_ungatedDrawers[i].Seqoid;
                s_allowanceEpoch++;
                long promised = s_promised.Remove(seqoid, out (long Promised, long Drawn) held) ? held.Promised : 0;
                CountDrawer(seqoid, -1, promised: -promised);
                if (promised != 0)
                {
                    _ = PgAtomic.Add(SequenceRanges.PromisedDraws, -promised);
                }

                s_ungatedDrawers.RemoveAt(i);
                _ = s_shareRuns.Remove(seqoid);
                if (s_drawingUngated.ContainsKey(seqoid))
                {
                    s_allowanceEpoch++;
                    s_drawingUngated[seqoid] = 0;
                    _ = s_sharesTaken.Remove(seqoid);
                }
            }
        }
    }

    // Counts the statement whose plan is starting among those drawing from sequences it draws from without their gates,
    // before it first checks them, so that statements drawing at once, each one value before it checks again, leave one
    // another values enough, until it ends.
    private static void CountUngated(List<uint> sequences)
    {
        if (sequences.Count == 0)
        {
            return;
        }

        RegisterEnd();
        uint subxact;
        unsafe
        {
            subxact = NativeMethods.GetCurrentSubTransactionId();
        }

        foreach (uint seqoid in sequences)
        {
            CountDrawer(seqoid, 1);
            s_ungatedDrawers.Add((s_starting.Count, 0, subxact, seqoid));
        }
    }

    // Counts this backend among those drawing from the sequences a PL/pgSQL function's own expression draws from, as a
    // statement whose plan names them is counted, from the expression's first call on, until the statement run the
    // function runs under ends, or, for a block outside any, as a DO block, until the block does.
    private static void CountFunction(List<uint> sequences)
    {
        RegisterEnd();
        int run = s_scopes.Count - 1;
        while (run >= 0 && s_scopes[run].Kind != RunScope)
        {
            run--;
        }

        nint statement = run < 0 ? 0 : s_scopes[run].Key;
        uint subxact;
        unsafe
        {
            subxact = NativeMethods.GetCurrentSubTransactionId();
        }

        foreach (uint seqoid in sequences.Where(seqoid => !s_ungatedDrawers.Exists(drawer => drawer.Depth == -1 && drawer.Statement == statement && drawer.Seqoid == seqoid)))
        {
            CountDrawer(seqoid, 1);
            s_ungatedDrawers.Add((-1, statement, subxact, seqoid));
        }
    }

    private static void ForgetUngatedDrawers()
    {
        foreach ((int, nint, uint, uint Seqoid) drawer in s_ungatedDrawers)
        {
            CountDrawer(drawer.Seqoid, -1);
        }

        s_ungatedDrawers.Clear();
    }

    // Has an upsert or a merge that holds gates check the sequences its rows draw from before each row its source gives,
    // as an insert's guard trigger does after each row it writes: a row that conflicts, or that a merge matches, fires no
    // such trigger, though its default, its SET and its triggers draw. A sequence a trigger's arguments name is checked as
    // the trigger fires.
    private static unsafe void CheckGatedRows(nint queryDesc)
    {
        QueryDesc* query = (QueryDesc*)queryDesc;
        if (!s_rowsDrawing.Remove(queryDesc, out List<uint>? drawing) || query->planstate is null || query->planstate->type != NodeTag.T_ModifyTableState ||
            query->planstate->plan is null ||
            (query->operation != CmdType.CMD_MERGE && ((ModifyTable*)query->planstate->plan)->onConflictAction == OnConflictAction.ONCONFLICT_NONE))
        {
            return;
        }

        PlanState* source = query->planstate->lefttree;
        if (source is null || s_gatedRows.ContainsKey((nint)source))
        {
            return;
        }

        s_gatedRows[(nint)source] = (source->ExecProcNodeReal, drawing);
        source->ExecProcNodeReal = GatedRowCallback;
        RegisterCounted();
        List<nint> nodes = s_countedNodes.TryGetValue(queryDesc, out (uint Subxact, List<nint> Nodes) counted) ? counted.Nodes : [];
        nodes.Add((nint)source);
        s_countedNodes[queryDesc] = (NativeMethods.GetCurrentSubTransactionId(), nodes);
    }

    // Produces an upsert's or a merge's next source row, after checking the sequences whose gates it holds: a switch
    // waiting for a gate passes, the draws the last row was seen to make count toward the next's, and a block too short
    // for them waits for its switch.
    private static unsafe TupleTableSlot* GatedRow(PlanState* node)
    {
        (ExecProcNodeMtd function, List<uint> sequences) = s_gatedRows[(nint)node];
        if (!s_namingGrantors && !Worf.InOwnSql)
        {
            List<uint> passed = PassGates();
            Refresh();
            Reload();
            foreach (uint seqoid in passed)
            {
                WaitWhileShort(seqoid);
            }

            foreach (uint seqoid in sequences.Where(s_gates.ContainsKey))
            {
                SeeRowDraws(seqoid);
                WaitWhileShort(seqoid);
            }
        }

        return function.Invoke(node);
    }

    // Has the guard count down before each row a plan node, and those under it, produce: its children, its initPlans
    // and subplans, as an ARRAY or scalar subquery runs, and the plans an Append, a MergeAppend or a subquery scan runs.
    private static unsafe void CountRows(PlanState* node, List<nint> nodes, int calls, bool running = false, IReadOnlyCollection<uint>? named = null,
        bool under = false, bool alone = false)
    {
        // A node that counts already has those under it count too, unless only the plan's top node did so far.
        if (node is null || (s_rowFunctions.ContainsKey((nint)node) && !under))
        {
            return;
        }

        if (!s_rowFunctions.ContainsKey((nint)node))
        {
            ExecProcNodeMtd real = node->ExecProcNodeReal;
            s_rowFunctions[(nint)node] = (real, calls, named);
            node->ExecProcNodeReal = CountedRowCallback;

            // A node that has produced a row runs its own function, as ExecProcNodeFirst left it, with no wrapper.
            if (running && node->ExecProcNode.DangerousGetAddress() == real.DangerousGetAddress())
            {
                node->ExecProcNode = CountedRowCallback;
            }

            nodes.Add((nint)node);
        }

        if (alone)
        {
            return;
        }

        CountRows(node->lefttree, nodes, calls, running, named, under);
        CountRows(node->righttree, nodes, calls, running, named, under);
        foreach (List* subplans in new[] { node->initPlan, node->subPlan })
        {
            for (int i = 0; subplans is not null && i < subplans->length; i++)
            {
                CountRows(((SubPlanState*)subplans->elements[i].ptr_value)->planstate, nodes, calls, running, named, under);
            }
        }

        switch (node->type)
        {
            case NodeTag.T_AppendState:
                for (int i = 0; i < ((AppendState*)node)->as_nplans; i++)
                {
                    CountRows(((AppendState*)node)->appendplans[i], nodes, calls, running, named, under);
                }

                break;
            case NodeTag.T_MergeAppendState:
                for (int i = 0; i < ((MergeAppendState*)node)->ms_nplans; i++)
                {
                    CountRows(((MergeAppendState*)node)->mergeplans[i], nodes, calls, running, named, under);
                }

                break;
            case NodeTag.T_SubqueryScanState:
                CountRows(((SubqueryScanState*)node)->subplan, nodes, calls, running, named, under);
                break;
            default:
                break;
        }
    }

    // Produces a plan node's next row, after counting down for the checks of the sequences the statement draws from.
    // The row is a unit: it holds its sequences' gates from its check until it's produced, and no longer, so that a
    // statement paused between its rows, as one whose client reads slowly, or a cursor between its FETCHes, holds none.
    private static unsafe TupleTableSlot* CountedRow(PlanState* node)
    {
        (ExecProcNodeMtd function, int calls, IReadOnlyCollection<uint>? named) = s_rowFunctions[(nint)node];

        // A statement that counts its rows down only for calls that started as it ran stops once they all go through
        // the guard themselves.
        if (s_runRows.Count > 0 && s_runRows.TryGetValue((nint)node, out nint running) && GuardRunCalls((QueryDesc*)running, node))
        {
            return function.Invoke(node);
        }

        // A Result with no plan under it gives one row a scan: once it has, its next call starts nothing.
        bool single = node->type == NodeTag.T_ResultState && node->lefttree is null;
        if (single && ((ResultState*)node)->rs_done)
        {
            return function.Invoke(node);
        }

        // A row under another, as one of many that a node gathers, is part of that row's unit, and the rows of a run
        // whose rows no client reads, as an INSERT's source or a function's query, keep their gates until the run ends,
        // passing them as a switch waits, as nothing but the rows' own work lies between them.
        if (s_scopes.Count > 0 && s_scopes[^1] is { Kind: RowScope } or { Kind: RunScope, Owner: not 0 })
        {
            CountDownDraws(calls, named, gate: s_scopes[^1].Kind == RunScope, single: single);
            return function.Invoke(node);
        }

        EnterScope(RowScope, (nint)node);
        bool failed = true;
        try
        {
            CountDownDraws(calls, named, single: single);
            TupleTableSlot* row = function.Invoke(node);
            failed = false;
            return row;
        }
        finally
        {
            LeaveScope(RowScope, (nint)node, failed: failed);
        }
    }

    // Runs as PL/pgSQL starts a statement, while it tells of them. The calls of nextval its expressions started since
    // it last told of one go through the guard from here on, and the statement of this function whose expression
    // started one has drawn, so its scope ends. A statement that starts within a unit that holds gates suspends it
    // until it ends, as it may run long without drawing.
    private static unsafe void PlpgsqlStatement(PLpgSQL_execstate* estate, PLpgSQL_stmt* stmt)
    {
        GuardSites();
        if (s_countingDown || s_namingGrantors || Worf.InOwnSql || estate is null || estate->func is null)
        {
            return;
        }

        EndDrawScope((nint)estate);
        if (s_ownsPlugin && s_unitGates.Count > 0)
        {
            EnterScope(StatementScope, (nint)stmt, (nint)estate);
        }
    }

    // Runs as a PL/pgSQL statement ends, while PL/pgSQL tells of that: as a statement's start does, and the scope the
    // statement started ends.
    private static unsafe void PlpgsqlStatementEnd(PLpgSQL_execstate* estate, PLpgSQL_stmt* stmt)
    {
        GuardSites();
        EndDrawScope((nint)estate);
        if (s_statementScopes > 0)
        {
            LeaveScope(StatementScope, (nint)stmt, (nint)estate);
        }
    }

    // Ends the scope of a function's statement whose expression started a call of nextval.
    private static void EndDrawScope(nint estate)
    {
        for (int i = s_scopes.Count - 1; s_statementScopes > 0 && i >= 0; i--)
        {
            if (s_scopes[i].Kind == DrawScope && s_scopes[i].Owner == estate)
            {
                LeaveScopeAt(i, false);
                return;
            }
        }
    }

    // Has PL/pgSQL tell worf of the statements it starts and ends, or no longer, through worf's own plugin: of both
    // while an expression that started a call has yet to have it go through the guard, or a scope that a statement's
    // end closes is open; and of those it starts while a unit holds gates for a statement to suspend. Otherwise
    // PL/pgSQL runs without a call into worf, each of which costs far more than a statement that does little.
    private static unsafe void WatchStatements()
    {
        if (s_plugin is null)
        {
            return;
        }

        bool ends = s_startedSites.Count > 0 || s_statementScopes > 0;
        bool starts = s_startedSites.Count > 0 || s_unitGates.Count > 0;
        if (starts != s_watchingStarts)
        {
            s_watchingStarts = starts;
            s_plugin->stmt_beg = starts ? PlpgsqlStatementCallback : default;
        }

        if (ends != s_watchingEnds)
        {
            s_watchingEnds = ends;
            s_plugin->stmt_end = ends ? PlpgsqlStatementEndCallback : default;
        }
    }

    // Whether PL/pgSQL tells worf of its statements: its plugin is worf's own, and still the one it looks at.
    private static unsafe bool TellsOfStatements() => s_ownsPlugin && *s_plpgsqlPlugin == s_plugin;

    // Notes a PL/pgSQL expression that starts a call of nextval, for the call to go through the guard once PL/pgSQL
    // next tells of a statement. An expression starts its calls once a transaction and makes them each time it runs,
    // as a loop's assignment does at every turn, without telling the guard of the runs after the first.
    private static unsafe void StartSite(nint simple, nint estate)
    {
        if (!TellsOfStatements() || estate == 0 || s_startedSites.Exists(site => site.Expr == simple))
        {
            return;
        }

        ReadNextval();
        RegisterEnd();
        HookExit();
        s_snowflake = Worf.CatalogOid("snowflake_node");
        PLpgSQL_function* function = ((PLpgSQL_execstate*)estate)->func;
        s_startedSites.Add((simple, NativeMethods.GetCurrentSubTransactionId(), estate, function is null || function->fn_oid == 0));
        WatchStatements();
    }

    // Has the calls of nextval that PL/pgSQL expressions started go through the guard each time they run from here
    // on, as PL/pgSQL tells of a statement.
    private static void GuardSites()
    {
        if (s_startedSites.Count == 0)
        {
            return;
        }

        // An expression that has yet to start, as one whose first call's check is still under way, waits for the next
        // statement.
        ReadHookedCall();
        _ = s_startedSites.RemoveAll(static site => GuardSite(site.Expr));
        WatchStatements();
    }

    // Has the calls of nextval of a PL/pgSQL expression that has started go through the guard. A call is made to go
    // through PostgreSQL's function that wraps a call in the fmgr hook, in which worf counts the draw without leaving
    // PostgreSQL's own call of nextval, or, where that hook isn't worf's, into GuardedNextval. One that names, with a
    // constant, a sequence that isn't managed stays as it is. Returns whether the expression has started.
    private static unsafe bool GuardSite(nint started)
    {
        PLpgSQL_expr* expr = (PLpgSQL_expr*)started;
        ExprState* state = expr->expr_simple_state;
        if (state is null || expr->expr_simple_lxid != Lxid())
        {
            return false;
        }

        for (int i = 0; i < state->steps_len; i++)
        {
            if (CallsNextval(state, &state->steps[i]))
            {
                _ = GuardCall(&state->steps[i]);
            }
        }

        return true;
    }

    // Whether a step of an expression's state calls nextval.
    private static unsafe bool CallsNextval(ExprState* state, ExprEvalStep* step)
        => step->d.func.nargs == 1 && (nint)step->d.func.fn_addr.DangerousGetAddress() == s_nextval &&
            (s_findingAlone || CallsFunction(StepKind(state, step)));

    // What each kind of step seen so far does, by how an expression's state marks it, which is the same for every
    // state of the backend: asked of PostgreSQL once for each, as a call into it for every step of every statement's
    // expressions costs more than the rest of what worf does with them.
    private static readonly Dictionary<long, ExprEvalOp> s_stepKinds = [];

    private static unsafe ExprEvalOp StepKind(ExprState* state, ExprEvalStep* step)
    {
        // A state the interpreter hasn't taken on yet, as one PostgreSQL is to compile, marks its steps by kind itself.
        if ((state->flags & InterpreterInitialized) == 0)
        {
            return NativeMethods.ExecEvalStepOp(state, step);
        }

        if (!s_stepKinds.TryGetValue(step->opcode, out ExprEvalOp kind))
        {
            kind = NativeMethods.ExecEvalStepOp(state, step);
            s_stepKinds[step->opcode] = kind;
        }

        return kind;
    }

    // Whether calls are looked for where worf may call nothing of PostgreSQL's, as within a memory context's reset: a
    // step that holds nextval's address where a call keeps its function is then taken for one, as no other step is
    // given it.
    private static bool s_findingAlone;

    // Has a call of nextval in an expression's state go through the guard each time it runs, and gives its place
    // among the calls that go through the fmgr hook, or -1 for one that doesn't.
    private static unsafe int GuardCall(ExprEvalStep* step)
    {
        Node* call = step->d.func.finfo->fn_expr;
        uint named = NamedByCall(call);
        if (named != 0 && (named == s_snowflake || !s_sequences.ContainsKey(named)))
        {
            return -1;
        }

        if (s_hookedCall == 0)
        {
            step->d.func.fn_addr = new PGFunction((void*)s_guardedNextval);
            return -1;
        }

        (uint, nint, nint) site = (named, (nint)step->d.func.fcinfo_data + sizeof(FunctionCallInfoBaseData), (nint)step->d.func.finfo);
        int index = s_sites.Count;
        if (s_freeSites.Count > 0)
        {
            index = s_freeSites.Pop();
            s_sites[index] = site;
        }
        else
        {
            s_sites.Add(site);
        }

        s_siteOf[(nint)call] = index;
        step->d.func.fn_addr = new PGFunction((void*)s_hookedCall);
        return index;
    }

    // Leaves the places a statement's calls took to the statements after it, as it ends.
    private static void FreePlanSites(nint queryDesc)
    {
        ReturnBrackets(queryDesc);
        if (s_planSites.Count == 0 || !s_planSites.Remove(queryDesc, out List<(int Site, nint Call, nint Information)>? sites))
        {
            return;
        }

        foreach ((int site, nint call, nint information) in sites)
        {
            _ = s_singleCalls.Remove(information);
            if (site < 0)
            {
                continue;
            }

            if (s_siteOf.TryGetValue(call, out int current) && current == site)
            {
                _ = s_siteOf.Remove(call);
            }

            s_sites[site] = default;
            s_freeSites.Push(site);
        }
    }

    // Finds PostgreSQL's nextval, and GuardedNextval as PostgreSQL calls it, once for the backend.
    private static unsafe void ReadNextval()
    {
        if (s_nextval == 0)
        {
            FmgrInfo nextval = default;
            NativeMethods.fmgr_info(NextvalOid, &nextval);
            s_nextval = (nint)nextval.fn_addr.DangerousGetAddress();
            s_guardedNextval = (nint)GuardedNextvalCallback.DangerousGetAddress();
        }
    }

    // PGJIT_EXPR, from jit/jit.h: PostgreSQL compiles the statement's expressions, where it has a JIT provider.
    private const int JitExpressions = 1 << 3;

    // The size of nodeFunctionscan.c's FunctionScanPerFuncState on a 64-bit server: the function's state, its result's
    // descriptor, its column count, its result set, its row count and its slot.
    private const int FunctionScanFunctionSize = 48;

    // EEO_FLAG_INTERPRETER_INITIALIZED, from executor/execExpr.h: PostgreSQL's interpreter runs the expression's
    // state. One that PostgreSQL compiled, with its calls' functions built in, never has it.
#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
    private const int InterpreterInitialized = 1 << 1;
#else
    private const int InterpreterInitialized = 1 << 5;
#endif

    // Whether PostgreSQL has compiled a call of this transaction's to go through the fmgr hook.
    private static bool s_compiledCalls;

    /// <summary>
    /// Tells PostgreSQL, as it compiles a call of nextval with JIT and asks whether the call needs the fmgr hook, to
    /// have the call go through it: a compiled call has its function built in, so the guard can't send it through
    /// the hook afterwards, as it does the interpreter's. That is asked while a statement starts that PostgreSQL
    /// compiles and that draws from a managed sequence without its switch gate, where the hook counts draws.
    /// </summary>
    /// <returns>Whether the call goes through the hook.</returns>
    internal static bool HooksCompiledCall()
    {
        if (s_starting.Count == 0 || s_hookedCall == 0 || s_namingGrantors)
        {
            return false;
        }

        WorfStartingStatement starting = s_starting[^1];
        if (!starting.Compiles || starting.Switched.Count == 0)
        {
            return false;
        }

        starting.Hooked++;
        s_compiledCalls = true;
        return true;
    }

    // Has the calls of nextval that a statement's plan started go through the guard each time they run, as a PL/pgSQL
    // expression's do, in place of a check before each row its nodes produce, which costs a call into worf a row.
    // That holds only where every call the plan started goes through the guard: one PostgreSQL's interpreter is to
    // run is found among the expression states the plan's nodes hold, in those of their conditions, their rows and an
    // aggregate's transitions, and one PostgreSQL compiled with JIT was compiled to go through the fmgr hook. Any
    // other statement counts its rows.
    private static unsafe bool GuardPlan(QueryDesc* query, WorfStartingStatement starting)
    {
        if (starting.Looked <= 0 || !Executor.CountsDraws)
        {
            return false;
        }

        ReadNextval();
        ReadHookedCall();
        if (s_hookedCall == 0)
        {
            return false;
        }

        // A statement marked for JIT is compiled only where the server has a JIT provider, so each state is asked.
        List<(nint Step, bool Single, bool Compiled)> calls = [];
        Dictionary<nint, nint> states = [];
        s_statesOf = states;
        try
        {
            FindCalls(query->planstate, calls, []);
        }
        finally
        {
            s_statesOf = null;
        }

        int interpreted = 0;
        foreach ((_, _, bool compiled) in calls)
        {
            interpreted += compiled ? 0 : 1;
        }

        if (interpreted + starting.Hooked != starting.Looked)
        {
            return false;
        }

        RegisterEnd();
        HookExit();
        s_snowflake = Worf.CatalogOid("snowflake_node");
        if (!s_planSites.TryGetValue((nint)query, out List<(int Site, nint Call, nint Information)>? sites))
        {
            sites = [];
        }

        List<(nint Step, uint Seqoid, ExprEvalStep Own)> draws = [];
        foreach ((nint call, bool single, _) in calls)
        {
            ExprEvalStep* step = (ExprEvalStep*)call;
            int site = GuardCall(step);
            if (single)
            {
                _ = s_singleCalls.Add((nint)step->d.func.fcinfo_data);
            }

            if (site >= 0 || single)
            {
                sites.Add((site, (nint)step->d.func.finfo->fn_expr, (nint)step->d.func.fcinfo_data));
            }

            // A call that names its sequence is bracketed where its expression allows.
            if (site >= 0 && s_sites[site].Item1 != 0)
            {
                ExprEvalStep own = *step;
                own.d.func.fn_addr = new PGFunction((void*)s_nextval);
                draws.Add((call, s_sites[site].Item1, own));
            }
        }

        if (sites.Count > 0)
        {
            s_planSites[(nint)query] = sites;
        }

        BracketDraws((nint)query, draws, states);
        return true;
    }

    // Finds the calls of nextval in the expression states a plan's nodes hold, through the nodes CountRows reaches:
    // each node's condition and row, a join's condition, a window function's arguments, filter and frame, an
    // aggregate's transitions and direct arguments, a set-returning function's arguments, a LIMIT's, a TABLESAMPLE's,
    // the conditions and arguments of the subqueries its expressions run, and what a table's writes return, check
    // or set on a conflict or in a MERGE.
    private static unsafe void FindCalls(PlanState* node, List<(nint Step, bool Single, bool Compiled)> calls, HashSet<nint> seen)
    {
        if (node is null || !seen.Add((nint)node))
        {
            return;
        }

        // A Result with no plan under it gives one row a scan.
        bool single = node->type == NodeTag.T_ResultState && node->lefttree is null;
        FindCalls(node->qual, calls, seen, single);
        if (node->ps_ProjInfo is not null)
        {
            FindCalls(&node->ps_ProjInfo->pi_state, calls, seen, single);
        }

        FindCalls(node->lefttree, calls, seen);
        FindCalls(node->righttree, calls, seen);
        foreach (List* subplans in new[] { node->initPlan, node->subPlan })
        {
            for (int i = 0; subplans is not null && i < subplans->length; i++)
            {
                SubPlanState* subplan = (SubPlanState*)subplans->elements[i].ptr_value;
                FindCalls(subplan->planstate, calls, seen);
                FindCalls(subplan->testexpr, calls, seen, false);
                FindCalls(subplan->projLeft, calls, seen);
                FindCalls(subplan->projRight, calls, seen);
#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
                FindCalls(subplan->args, calls, seen);
#endif
            }
        }

        switch (node->type)
        {
            case NodeTag.T_NestLoopState:
            case NodeTag.T_MergeJoinState:
            case NodeTag.T_HashJoinState:
                FindCalls(((JoinState*)node)->joinqual, calls, seen, false);
                break;
            case NodeTag.T_WindowAggState:
                WindowAggState* window = (WindowAggState*)node;
                for (int i = 0; window->funcs is not null && i < window->funcs->length; i++)
                {
                    WindowFuncExprState* function = (WindowFuncExprState*)window->funcs->elements[i].ptr_value;
                    FindCalls(function->args, calls, seen);
                    FindCalls(function->aggfilter, calls, seen, false);
                }

                FindCalls(window->startOffset, calls, seen, false);
                FindCalls(window->endOffset, calls, seen, false);
                FindCalls(window->runcondition, calls, seen, false);
                break;
            case NodeTag.T_LimitState:
                FindCalls(((LimitState*)node)->limitOffset, calls, seen, false);
                FindCalls(((LimitState*)node)->limitCount, calls, seen, false);
                break;
            case NodeTag.T_SampleScanState:
                FindCalls(((SampleScanState*)node)->args, calls, seen);
                FindCalls(((SampleScanState*)node)->repeatable, calls, seen, false);
                break;
            case NodeTag.T_ValuesScanState:
                // The rows whose expressions run subqueries, which PostgreSQL builds as the statement starts.
                ValuesScanState* values = (ValuesScanState*)node;
                for (int i = 0; values->exprstatelists is not null && i < values->array_len; i++)
                {
                    FindCalls(values->exprstatelists[i], calls, seen);
                }

                break;
            case NodeTag.T_FunctionScanState:
                // What a function scan keeps for each of its functions, as ROWS FROM lists several, no header declares:
                // nodeFunctionscan.c's FunctionScanPerFuncState, the same from PostgreSQL 15 to 19, whose first field is
                // the function's state.
                FunctionScanState* scan = (FunctionScanState*)node;
                for (int i = 0; scan->funcstates is not null && i < scan->nfuncs; i++)
                {
                    FindCalls(*(SetExprState**)((byte*)scan->funcstates + (i * FunctionScanFunctionSize)), calls, seen);
                }

                break;
            case NodeTag.T_ModifyTableState:
                ModifyTableState* modify = (ModifyTableState*)node;
                for (int i = 0; modify->resultRelInfo is not null && i < modify->mt_nrels; i++)
                {
                    FindWriteCalls(&modify->resultRelInfo[i], calls, seen);
                }

                break;
            case NodeTag.T_AppendState:
                for (int i = 0; i < ((AppendState*)node)->as_nplans; i++)
                {
                    FindCalls(((AppendState*)node)->appendplans[i], calls, seen);
                }

                break;
            case NodeTag.T_MergeAppendState:
                for (int i = 0; i < ((MergeAppendState*)node)->ms_nplans; i++)
                {
                    FindCalls(((MergeAppendState*)node)->mergeplans[i], calls, seen);
                }

                break;
            case NodeTag.T_SubqueryScanState:
                FindCalls(((SubqueryScanState*)node)->subplan, calls, seen);
                break;
            case NodeTag.T_ResultState:
                FindCalls(((ResultState*)node)->resconstantqual, calls, seen, single);
                break;
            case NodeTag.T_ProjectSetState:
                for (int i = 0; i < ((ProjectSetState*)node)->nelems; i++)
                {
                    Node* elem = ((ProjectSetState*)node)->elems[i];
                    if (elem is not null && elem->type == NodeTag.T_ExprState)
                    {
                        FindCalls((ExprState*)elem, calls, seen, false);
                    }
                    else
                    {
                        FindCalls((SetExprState*)elem, calls, seen);
                    }
                }

                break;
            case NodeTag.T_AggState:
                for (int i = 0; i < ((AggState*)node)->numphases; i++)
                {
                    AggStatePerPhaseData* phase = &((AggState*)node)->phases[i];
                    FindCalls(phase->evaltrans, calls, seen, false);
                    ExprState** cached = (ExprState**)&phase->evaltrans_cache;
                    for (int j = 0; j < 4; j++)
                    {
                        FindCalls(cached[j], calls, seen, false);
                    }
                }

                for (int i = 0; ((AggState*)node)->peragg is not null && i < ((AggState*)node)->numaggs; i++)
                {
                    FindCalls(((AggState*)node)->peragg[i].aggdirectargs, calls, seen);
                }

                break;
            default:
                break;
        }
    }

    // Finds the calls in the expression states of a list.
    private static unsafe void FindCalls(List* states, List<(nint Step, bool Single, bool Compiled)> calls, HashSet<nint> seen)
    {
        for (int i = 0; states is not null && states->type == NodeTag.T_List && i < states->length; i++)
        {
            Node* state = (Node*)states->elements[i].ptr_value;
            if (state is not null && state->type == NodeTag.T_ExprState)
            {
                FindCalls((ExprState*)state, calls, seen, false);
            }
        }
    }

    // Finds the calls in a projection's expression state.
    private static unsafe void FindCalls(ProjectionInfo* projection, List<(nint Step, bool Single, bool Compiled)> calls, HashSet<nint> seen)
    {
        if (projection is not null)
        {
            FindCalls(&projection->pi_state, calls, seen, false);
        }
    }

    // Finds the calls in the arguments of a function that returns a set, or in the expression that stands for it.
    private static unsafe void FindCalls(SetExprState* function, List<(nint Step, bool Single, bool Compiled)> calls, HashSet<nint> seen)
    {
        if (function is not null && function->type == NodeTag.T_SetExprState)
        {
            FindCalls(function->args, calls, seen);
            FindCalls(function->elidedFuncState, calls, seen, false);
        }
    }

    // Finds the calls in the conditions and the rows of a MERGE's actions.
    private static unsafe void FindMergeCalls(List* actions, List<(nint Step, bool Single, bool Compiled)> calls, HashSet<nint> seen)
    {
        for (int i = 0; actions is not null && i < actions->length; i++)
        {
            MergeActionState* action = (MergeActionState*)actions->elements[i].ptr_value;
            FindCalls(action->mas_proj, calls, seen);
            FindCalls(action->mas_whenqual, calls, seen, false);
        }
    }

    // Finds the calls in the expression states a table a statement writes keeps for its rows: its CHECK constraints,
    // its generated columns and its triggers' WHEN conditions, which start with the table's first row, and, for a
    // partition a row is routed to, what its rows return, check or set on a conflict or in a MERGE.
    private static unsafe void FindCalls(ResultRelInfo* written, bool routed, List<(nint Step, bool Single, bool Compiled)> calls, HashSet<nint> seen)
    {
        // A table the statement has closed already may be gone.
        TupleDescData* row = written->ri_RelationDesc is null || written->ri_RelationDesc->rd_refcnt <= 0 ? null : written->ri_RelationDesc->rd_att;
        if (row is null)
        {
            return;
        }

#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
        ExprState** checks = written->ri_ConstraintExprs;
#else
        ExprState** checks = written->ri_CheckConstraintExprs;
#endif
        for (int i = 0; checks is not null && row->constr is not null && i < row->constr->num_check; i++)
        {
            FindCalls(checks[i], calls, seen, false);
        }

#if ANKUS_PG15
        ExprState**[] generated = [written->ri_GeneratedExprs];
#else
        ExprState**[] generated = [written->ri_GeneratedExprsI, written->ri_GeneratedExprsU];
#endif
        foreach (ExprState** columns in generated)
        {
            for (int i = 0; columns is not null && i < row->natts; i++)
            {
                FindCalls(columns[i], calls, seen, false);
            }
        }

        for (int i = 0; written->ri_TrigWhenExprs is not null && written->ri_TrigDesc is not null && i < written->ri_TrigDesc->numtriggers; i++)
        {
            FindCalls(written->ri_TrigWhenExprs[i], calls, seen, false);
        }

        if (routed)
        {
            FindWriteCalls(written, calls, seen);
        }
    }

    // Finds the calls in what a table's writes return, check for a view, or set on a conflict or in a MERGE.
    private static unsafe void FindWriteCalls(ResultRelInfo* written, List<(nint Step, bool Single, bool Compiled)> calls, HashSet<nint> seen)
    {
        FindCalls(written->ri_projectReturning, calls, seen);
        FindCalls(written->ri_WithCheckOptionExprs, calls, seen);
        if (written->ri_onConflict is not null)
        {
            FindCalls(written->ri_onConflict->oc_ProjInfo, calls, seen);
            FindCalls(written->ri_onConflict->oc_WhereClause, calls, seen, false);
        }

#if ANKUS_PG15 || ANKUS_PG16
        FindMergeCalls(written->ri_matchedMergeAction, calls, seen);
        FindMergeCalls(written->ri_notMatchedMergeAction, calls, seen);
#else
        // One list for each of MERGE's three kinds of match.
        List** actions = (List**)&written->ri_MergeActions;
        for (int kind = 0; kind < 3; kind++)
        {
            FindMergeCalls(actions[kind], calls, seen);
        }

        FindCalls(written->ri_MergeJoinCondition, calls, seen, false);
#endif
    }

    // Has the calls of nextval that a statement started as it ran go through the guard each time they run, once they
    // are found in the states its tables keep, and its rows count down no more once every one of them does, but for
    // those the client reads of a statement with a VALUES list. A call that PostgreSQL compiled, or whose state is
    // kept elsewhere, as a domain's CHECK constraint's may be, is never found, and the statement goes on checking
    // before each row. Tells whether the node whose row is due counts no more.
    private static unsafe bool GuardRunCalls(QueryDesc* query, PlanState* due)
    {
        int started = s_runStarted.GetValueOrDefault((nint)query);
        if (started == 0 || query->estate is null || !Executor.CountsDraws)
        {
            return false;
        }

        if (!s_runFound.TryGetValue((nint)query, out (HashSet<nint> Found, int Looked) found))
        {
            found = ([], 0);
        }

        if (found.Looked == started)
        {
            return false;
        }

        ReadNextval();
        ReadHookedCall();
        if (s_hookedCall == 0)
        {
            return false;
        }

        RegisterEnd();
        HookExit();
        s_snowflake = Worf.CatalogOid("snowflake_node");
        if (!s_planSites.TryGetValue((nint)query, out List<(int Site, nint Call, nint Information)>? sites))
        {
            sites = [];
        }

        foreach ((nint call, _, bool compiled) in RunCalls(query->estate))
        {
            if (compiled || !found.Found.Add(call))
            {
                continue;
            }

            ExprEvalStep* step = (ExprEvalStep*)call;
            int site = GuardCall(step);
            if (site >= 0)
            {
                sites.Add((site, (nint)step->d.func.finfo->fn_expr, (nint)step->d.func.fcinfo_data));
            }
        }

        if (sites.Count > 0)
        {
            s_planSites[(nint)query] = sites;
        }

        s_runFound[(nint)query] = (found.Found, started);
        if (found.Found.Count != started)
        {
            return false;
        }

        // A statement whose VALUES rows go to the client has its top node go on counting, which lets their gates go.
        bool top = s_valuesRows.TryGetValue((nint)query, out (nint Memory, nint Node)[]? values) && values.Length > 0 && !RunKeepsGates((nint)query);
        StopCountingRows(query, top);
        if (top && due == query->planstate)
        {
            return false;
        }

        LetRunGates((nint)query);
        return true;
    }

    // Finds the calls in the expression states the tables a running statement writes keep for their rows.
    private static unsafe List<(nint Step, bool Single, bool Compiled)> RunCalls(EState* estate)
    {
        List<(nint Step, bool Single, bool Compiled)> calls = [];
        HashSet<nint> seen = [];
        for (int i = 0; estate->es_opened_result_relations is not null && i < estate->es_opened_result_relations->length; i++)
        {
            FindCalls((ResultRelInfo*)estate->es_opened_result_relations->elements[i].ptr_value, false, calls, seen);
        }

        for (int i = 0; estate->es_tuple_routing_result_relations is not null && i < estate->es_tuple_routing_result_relations->length; i++)
        {
            FindCalls((ResultRelInfo*)estate->es_tuple_routing_result_relations->elements[i].ptr_value, true, calls, seen);
        }

        for (int i = 0; estate->es_trig_target_relations is not null && i < estate->es_trig_target_relations->length; i++)
        {
            FindCalls((ResultRelInfo*)estate->es_trig_target_relations->elements[i].ptr_value, false, calls, seen);
        }

        return calls;
    }

    // Has a running statement's nodes count their rows down no more, but for its plan's top node where asked.
    private static unsafe void StopCountingRows(QueryDesc* query, bool top)
    {
        if (!s_countedNodes.Remove((nint)query, out (uint Subxact, List<nint> Nodes) counted))
        {
            return;
        }

        void* counts = CountedRowCallback.DangerousGetAddress();
        List<nint> kept = [];
        foreach (nint counting in counted.Nodes)
        {
            if ((top && counting == (nint)query->planstate) || !s_runRows.Remove(counting) ||
                !s_rowFunctions.Remove(counting, out (ExecProcNodeMtd Function, int Calls, IReadOnlyCollection<uint>? Named) row))
            {
                kept.Add(counting);
                continue;
            }

            PlanState* node = (PlanState*)counting;
            if (node->ExecProcNode.DangerousGetAddress() == counts)
            {
                node->ExecProcNode = row.Function;
            }

            node->ExecProcNodeReal = row.Function;
        }

        if (kept.Count > 0)
        {
            s_countedNodes[(nint)query] = (counted.Subxact, kept);
        }

        if (top && kept.Contains((nint)query->planstate))
        {
            _ = s_runTop.Add((nint)query);
        }
    }

    // Lets go the gates a statement's run and the rows under way in it hold, as a row's start does those of its run.
    private static void LetRunGates(nint queryDesc)
    {
        int run = s_scopes.Count - 1;
        while (run >= 0 && (s_scopes[run].Kind != RunScope || s_scopes[run].Key != queryDesc))
        {
            run--;
        }

        if (run < 0 || s_scopeGates.Count <= s_scopes[run].First)
        {
            return;
        }

        int first = s_scopes[run].First;
        for (int i = s_scopeGates.Count - 1; i >= first; i--)
        {
            if (s_unitGates.Contains(s_scopeGates[i]))
            {
                LetGate(s_scopeGates[i]);
            }
        }

        s_scopeGates.RemoveRange(first, s_scopeGates.Count - first);
        for (int i = run; i < s_scopes.Count; i++)
        {
            s_scopes[i] = s_scopes[i] with { First = Math.Min(s_scopes[i].First, first) };
        }
    }

    private static unsafe void FindCalls(ExprState* state, List<(nint Step, bool Single, bool Compiled)> calls, HashSet<nint> seen, bool single)
    {
        if (state is null || !seen.Add((nint)state))
        {
            return;
        }

        bool compiled = (state->flags & InterpreterInitialized) == 0;
        for (int i = 0; i < state->steps_len; i++)
        {
            // A draw a bracket holds goes through the guard already, in whichever of its forms it stands.
            if (i > 0 && s_callbackCode != 0 && state->steps[i - 1].opcode == s_callbackCode &&
                state->steps[i - 1].d.cparam.paramfunc.DangerousGetAddress() == (delegate* unmanaged[Cdecl]<ExprState*, ExprEvalStep*, ExprContext*, void>)&DrawStep)
            {
                continue;
            }

            if (s_findingIdentities)
            {
                if (StepKind(state, &state->steps[i]) != ExprEvalOp.EEOP_NEXTVALUEEXPR)
                {
                    continue;
                }

                // An expression PostgreSQL is to compile calls the draw's own function from the code it builds, so
                // the one expression that draws for an identity column goes back to the interpreter, whose step
                // can become a call: PostgreSQL has built no code for the statement yet as its plan starts.
                if (compiled)
                {
                    NativeMethods.ExecReadyInterpretedExpr(state);
                    compiled = (state->flags & InterpreterInitialized) == 0;
                }

                calls.Add(((nint)(&state->steps[i]), single, compiled));
                s_statesOf?.TryAdd((nint)(&state->steps[i]), (nint)state);
            }
            else if (CallsNextval(state, &state->steps[i]))
            {
                calls.Add(((nint)(&state->steps[i]), single, compiled));
                s_statesOf?.TryAdd((nint)(&state->steps[i]), (nint)state);
            }
        }
    }

    // The expression states of the calls being found, by where each call's step is, while a statement's calls are
    // found to be bracketed.
    private static Dictionary<nint, nint>? s_statesOf;

    // The brackets each statement, or COPY, has put around its draws, and those free to use again.
    private static readonly Dictionary<nint, List<nint>> s_brackets = [];
    private static readonly Stack<nint> s_freeBrackets = new();

    // How a step PostgreSQL's interpreter runs is marked as one that calls a function it is given, and as one that
    // stores a constant, and the interpreter's own function, each read once for the backend.
    private static long s_callbackCode;
    private static long s_constantCode;
    private static long s_valueCode;
    private static nint s_interpreter;
    private static bool s_stepCodesRead;

    // Whether each kind of step seen so far, as an expression's state marks it, jumps to another step, by what none
    // of them does that the guard brackets a draw among.
    private static readonly Dictionary<long, bool> s_stepsInLine = [];

    // The draws a bracket's step takes ahead at once.
    private const int BracketBatch = 64;

    // Reads how the interpreter marks the steps a bracket adds, from an expression's state made of them.
    private static unsafe bool ReadStepCodes()
    {
        if (!s_stepCodesRead)
        {
            ExprState* made = (ExprState*)NativeMethods.palloc0((nuint)sizeof(ExprState));
            made->type = NodeTag.T_ExprState;
            made->steps = (ExprEvalStep*)NativeMethods.palloc0((nuint)(4 * sizeof(ExprEvalStep)));
            made->steps_len = 4;
            made->steps_alloc = 4;
            made->steps[0].opcode = (long)ExprEvalOp.EEOP_PARAM_CALLBACK;
            made->steps[1].opcode = (long)ExprEvalOp.EEOP_CONST;
            made->steps[2].opcode = (long)ExprEvalOp.EEOP_CASE_TESTVAL;
#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
            made->steps[3].opcode = (long)ExprEvalOp.EEOP_DONE;
#else
            made->steps[3].opcode = (long)ExprEvalOp.EEOP_DONE_RETURN;
#endif
            for (int i = 0; i < 4; i++)
            {
                made->steps[i].resvalue = &made->resvalue;
                made->steps[i].resnull = &made->resnull;
            }

            NativeMethods.ExecReadyInterpretedExpr(made);
            s_callbackCode = made->steps[0].opcode;
            s_constantCode = made->steps[1].opcode;
            s_valueCode = made->steps[2].opcode;
            s_interpreter = (nint)made->evalfunc_private;
            NativeMethods.pfree(made->steps);
            NativeMethods.pfree(made);
            s_stepCodesRead = true;
        }

        return s_interpreter != 0;
    }

    // Whether none of an expression's steps jumps to another, so that steps put among them leave every step going
    // on to the one after it, as before.
    private static unsafe bool StepsInLine(ExprState* state)
    {
        for (int i = 0; i < state->steps_len; i++)
        {
            long code = state->steps[i].opcode;
            if (!s_stepsInLine.TryGetValue(code, out bool inLine))
            {
                inLine = StepKind(state, &state->steps[i]) is
#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
                    ExprEvalOp.EEOP_DONE or
#else
                    ExprEvalOp.EEOP_DONE_RETURN or ExprEvalOp.EEOP_DONE_NO_RETURN or ExprEvalOp.EEOP_OLD_FETCHSOME or ExprEvalOp.EEOP_NEW_FETCHSOME or
                    ExprEvalOp.EEOP_OLD_VAR or ExprEvalOp.EEOP_NEW_VAR or ExprEvalOp.EEOP_ASSIGN_OLD_VAR or ExprEvalOp.EEOP_ASSIGN_NEW_VAR or
                    ExprEvalOp.EEOP_FUNCEXPR_STRICT_1 or ExprEvalOp.EEOP_FUNCEXPR_STRICT_2 or
#endif
                    ExprEvalOp.EEOP_INNER_FETCHSOME or ExprEvalOp.EEOP_OUTER_FETCHSOME or ExprEvalOp.EEOP_SCAN_FETCHSOME or
                    ExprEvalOp.EEOP_INNER_VAR or ExprEvalOp.EEOP_OUTER_VAR or ExprEvalOp.EEOP_SCAN_VAR or
                    ExprEvalOp.EEOP_ASSIGN_INNER_VAR or ExprEvalOp.EEOP_ASSIGN_OUTER_VAR or ExprEvalOp.EEOP_ASSIGN_SCAN_VAR or
                    ExprEvalOp.EEOP_ASSIGN_TMP or ExprEvalOp.EEOP_ASSIGN_TMP_MAKE_RO or ExprEvalOp.EEOP_CONST or
                    ExprEvalOp.EEOP_FUNCEXPR or ExprEvalOp.EEOP_FUNCEXPR_STRICT or ExprEvalOp.EEOP_FUNCEXPR_FUSAGE or ExprEvalOp.EEOP_FUNCEXPR_STRICT_FUSAGE or
                    ExprEvalOp.EEOP_PARAM_EXEC or ExprEvalOp.EEOP_PARAM_EXTERN or ExprEvalOp.EEOP_PARAM_CALLBACK or ExprEvalOp.EEOP_NEXTVALUEEXPR;
                if (s_stepsInLine.Count >= 512)
                {
                    s_stepsInLine.Clear();
                }

                s_stepsInLine[code] = inLine;
            }

            if (!inLine)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Brackets a draw among an expression's steps, where the fmgr hook counts draws: a step before it, which
    /// PostgreSQL calls straight, counts the draw among those under way and against the draws its backend may take
    /// before it checks the sequence again, and a step after it counts it under way no more, by storing a zero where
    /// the backend's count is. The draw between them is then PostgreSQL's own, with no call through the fmgr hook, and
    /// an identity column's with no call at all. Whenever the draw can't go on at once, the step before it puts the
    /// call that goes through the guard in its place, which does as it did before. That holds for an expression the
    /// interpreter runs, none of whose steps jumps to another, with room for the two steps.
    /// </summary>
    /// <param name="state">The expression's state.</param>
    /// <param name="step">The draw's step, as a call that goes through the guard.</param>
    /// <param name="seqoid">The sequence it draws from.</param>
    /// <param name="own">The draw's step as PostgreSQL made it.</param>
    /// <returns>The bracket, or zero where the draw stays as it is.</returns>
    private static unsafe nint BracketDraw(ExprState* state, ExprEvalStep* step, uint seqoid, ExprEvalStep own)
    {
        int index = (int)(step - state->steps);
        if (s_fastSlot is null || !ReadStepCodes() || (state->flags & InterpreterInitialized) == 0 || (nint)state->evalfunc_private != s_interpreter ||
            index < 0 || index >= state->steps_len || state->steps_len + 2 > state->steps_alloc || !StepsInLine(state))
        {
            return 0;
        }

        return PutBracket(state, index, seqoid, own, *step);
    }

    // Puts a bracket's two steps around the draw at a step of an expression, which has room for them, and leaves
    // the draw in the form that goes through the guard.
    private static unsafe nint PutBracket(ExprState* state, int index, uint seqoid, ExprEvalStep own, ExprEvalStep guarded)
    {
        WorfDrawBracket* bracket = s_freeBrackets.Count > 0 ? (WorfDrawBracket*)s_freeBrackets.Pop() : (WorfDrawBracket*)NativeMemory.Alloc((nuint)sizeof(WorfDrawBracket));
        *bracket = default;
        bracket->Seqoid = seqoid;
        bracket->Own = own;
        bracket->Guarded = guarded;
        ExprEvalStep* steps = state->steps;
        Buffer.MemoryCopy(&steps[index + 1], &steps[index + 3], (long)(state->steps_alloc - index - 3) * sizeof(ExprEvalStep),
            (long)(state->steps_len - index - 1) * sizeof(ExprEvalStep));
        steps[index + 1] = bracket->Guarded;
        steps[index] = default;
        steps[index].opcode = s_callbackCode;
        steps[index].resvalue = bracket->Guarded.resvalue;
        steps[index].resnull = bracket->Guarded.resnull;
        steps[index].d.cparam.paramfunc = new ExecEvalSubroutine((delegate* unmanaged[Cdecl]<ExprState*, ExprEvalStep*, ExprContext*, void>)&DrawStep);
        steps[index].d.cparam.paramarg = bracket;
        steps[index + 2] = default;
        steps[index + 2].opcode = s_constantCode;
        steps[index + 2].resvalue = (ulong*)s_fastSlot;
        steps[index + 2].resnull = &bracket->Unread;
        state->steps_len += 2;
        return (nint)bracket;
    }

    // The draws found and those bracketed as a statement's plan started, by the expression's state each is in, the
    // step it stood at before any bracket, its sequence and whether it is an identity column's.
    private static int s_drawsFound;
    private static readonly List<(nint State, int Step, uint Seqoid, bool Identity)> s_drawsBracketed = [];

    // How often a table, a type, a function or a schema has changed, as this backend heard of it.
    private static long s_tablesStamp;

    // The INSERTs of one row that have run in full, by their plans, the one starting now that goes by what is kept of
    // it, and those running that do, which a failure within a subtransaction forgets.
    private static readonly Dictionary<nint, WorfSettledInsert> s_settled = [];
    private static (nint Query, WorfSettledInsert? Insert) s_settledStarting;
    private static readonly List<nint> s_settledRunning = [];

    // A hash of a statement's own text, as its place in the text it came in gives it.
    private static unsafe long TextHash(nint source, int location, int length)
    {
        byte* start = (byte*)source + Math.Max(location, 0);
        ReadOnlySpan<byte> text = length > 0 ? new ReadOnlySpan<byte>(start, length) : MemoryMarshal.CreateReadOnlySpanFromNullTerminated(start);
        ulong hash = 14695981039346656037;
        foreach (byte one in text)
        {
            hash = (hash ^ one) * 1099511628211;
        }

        return (long)hash;
    }

    // The statement run that a statement starting now runs within, as CountFunction finds it, or zero.
    private static nint RunAround()
    {
        for (int i = s_scopes.Count - 1; i >= 0; i--)
        {
            if (s_scopes[i].Kind == RunScope)
            {
                return s_scopes[i].Key;
            }
        }

        return 0;
    }

    /// <summary>
    /// Keeps what an INSERT of one row whose guarded columns take their defaults came to, as its plan has started in
    /// full: its every draw stands bracketed in the expression that works the row out, the statement makes no other
    /// call of nextval, and its plan is one PostgreSQL neither compiles nor makes anew for the values it is given.
    /// </summary>
    private static unsafe void SettleInsert(QueryDesc* query, WorfStartingStatement starting, short verifies)
    {
        PlannedStmt* planned = query->plannedstmt;
        PlanState* rows = query->planstate is null ? null : query->planstate->lefttree;
        if (query->operation != CmdType.CMD_INSERT || query->sourceText is null || planned is null || rows is null || rows->type != NodeTag.T_ResultState ||
            rows->lefttree is not null || rows->ps_ProjInfo is null || starting.Compiles || starting.Explained || planned->jitFlags != 0 ||
            !InsertsRowsAsGiven(planned) || CallsUserFunctions(planned) || s_drawsBracketed.Count == 0 || s_drawsBracketed.Count != s_drawsFound ||
            starting.Draws.Unnamed || starting.Draws.Calls != s_drawsBracketed.Count(draw => !draw.Identity) || starting.Looked != starting.Draws.Calls ||
            s_drawsBracketed.Exists(draw => draw.State != (nint)(&rows->ps_ProjInfo->pi_state)) || s_version < 0)
        {
            return;
        }

        if (s_settled.Count >= 256)
        {
            s_settled.Clear();
        }

        s_settled[(nint)planned] = new()
        {
            Tree = (nint)planned->planTree,
            Text = ((nint)query->sourceText, planned->stmt_location, planned->stmt_len),
            Hash = TextHash((nint)query->sourceText, planned->stmt_location, planned->stmt_len),
            Stamp = (s_tablesStamp, s_version, s_localVersion),
            SearchPath = SearchPath(),
            Draws = [.. s_drawsBracketed.OrderByDescending(draw => draw.Step).Select(draw => (draw.Step, draw.Seqoid, draw.Identity))],
            Verifies = verifies,
        };
    }

    /// <summary>
    /// Checks, as a statement starts, whether it is an INSERT of one row that ran in full before and can go by what
    /// the guard kept of it: its plan and its text are the ones kept, no table, type, function, schema or block has
    /// changed since, and each sequence it draws from stands as its last run left it within the command around it,
    /// with draws left to take, counted for that command, its grantor heard from, and no gate of it held. The
    /// statement then reads nothing of its plan or its tables: its draws are bracketed where they stood before, and
    /// each call of its that starts is passed as the guard passed it then.
    /// </summary>
    /// <param name="queryDesc">The statement.</param>
    /// <param name="explained">Whether the statement is only explained.</param>
    /// <returns>Whether it goes by what the guard kept.</returns>
    internal static unsafe bool StartsSettled(QueryDesc* queryDesc, bool explained)
    {
        PlannedStmt* planned = queryDesc->plannedstmt;
        if (s_settled.Count == 0 || planned is null || explained || !s_settled.TryGetValue((nint)planned, out WorfSettledInsert? kept))
        {
            return false;
        }

        if (kept.Tree != (nint)planned->planTree || kept.Text != ((nint)queryDesc->sourceText, planned->stmt_location, planned->stmt_len) ||
            kept.Stamp != (s_tablesStamp, s_version, s_localVersion) || s_version < 0 || s_fastSlot is null ||
            Volatile.Read(ref s_fastDraws[WorfFastDraws.Changes]) != s_changesRead || SequenceRanges.LocalVersion != s_localVersion ||
            s_gatingAfterAll || s_namingGrantors || s_countingDown || Worf.InOwnSql || s_gating.Count != 0 || s_gates.Count != 0 ||
            NativeGlobals.creating_extension || NativeGlobals.XactReadOnly || !Within() || !ReferenceEquals(SearchPath(), kept.SearchPath) ||
            kept.Hash != TextHash((nint)queryDesc->sourceText, planned->stmt_location, planned->stmt_len))
        {
            return false;
        }

        nint run = RunAround();
        foreach ((_, uint seqoid, _) in kept.Draws)
        {
            if (!s_sequences.ContainsKey(seqoid) || s_moved.Contains(seqoid) || !s_drawingUngated.ContainsKey(seqoid) || !s_drewWithin.Contains(seqoid) ||
                !s_drawAgainWithin.Contains(seqoid) || (s_grantors.ContainsKey(seqoid) && !s_confirmedSequences.Contains(seqoid)))
            {
                return false;
            }

            bool counted = false;
            foreach ((int depth, nint statement, _, uint drawn) in s_ungatedDrawers)
            {
                counted |= depth == -1 && statement == run && drawn == seqoid;
            }

            if (!counted)
            {
                return false;
            }
        }

        s_settledStarting = ((nint)queryDesc, kept);
        return true;
    }

    /// <summary>
    /// Brackets the draws of a statement that goes by what the guard kept of it, once its plan has started, where
    /// they stood before, and sees to the guard's triggers on its table. A draw is PostgreSQL's own while it may go
    /// on at once, and otherwise a call that the guard takes whole, as it takes a call of a PL/pgSQL expression's
    /// that it couldn't count: one that takes the sequence's gate, counts down and waits for a switch. Where a step
    /// isn't the draw kept, every call of the plan's is such a call.
    /// </summary>
    /// <param name="queryDesc">The statement.</param>
    /// <param name="started">Whether its plan started.</param>
    internal static unsafe void StartedSettled(nint queryDesc, bool started)
    {
        WorfSettledInsert? kept = s_settledStarting.Query == queryDesc ? s_settledStarting.Insert : null;
        s_settledStarting = default;
        if (!started || kept is null)
        {
            return;
        }

        QueryDesc* query = (QueryDesc*)queryDesc;
        PlanState* rows = query->planstate is null ? null : query->planstate->lefttree;
        ExprState* works = rows is null || rows->ps_ProjInfo is null ? null : &rows->ps_ProjInfo->pi_state;
        ReadNextval();
        bool stands = works is not null && ReadStepCodes() && (works->flags & InterpreterInitialized) != 0 && (nint)works->evalfunc_private == s_interpreter &&
            works->steps_len + (2 * kept.Draws.Length) <= works->steps_alloc;
        foreach ((int step, uint seqoid, bool identity) in kept.Draws)
        {
            stands = stands && step < works->steps_len && (identity
                ? StepKind(works, &works->steps[step]) == ExprEvalOp.EEOP_NEXTVALUEEXPR && works->steps[step].d.nextvalueexpr.seqid == seqoid
                : CallsNextval(works, &works->steps[step])) && (!identity || ReadDraw());
        }

        RegisterEnd();
        if (stands)
        {
            List<nint> made = [];
            foreach ((int step, uint seqoid, bool identity) in kept.Draws)
            {
                ExprEvalStep own = works->steps[step];
                ExprEvalStep guarded = own;
                if (identity)
                {
                    guarded = WholeDraw(own, seqoid);
                }
                else
                {
                    guarded.d.func.fn_addr = new PGFunction((void*)s_guardedNextval);
                }

                nint bracket = PutBracket(works, step, seqoid, own, guarded);
                made.Add(bracket);
                ((WorfDrawBracket*)bracket)->Direct = true;
                works->steps[step + 1] = own;
            }

            s_brackets[queryDesc] = made;
        }
        else
        {
            GuardWhole(query);
        }

        s_settledRunning.Add(queryDesc);
        SettleGuards(query, kept.Verifies == 0, kept.Verifies);
    }

    // Has every call of nextval and every identity column's draw in a plan go into the guard whole, each time it runs.
    private static unsafe void GuardWhole(QueryDesc* query)
    {
        List<(nint Step, bool Single, bool Compiled)> calls = [];
        FindCalls(query->planstate, calls, []);
        foreach ((nint call, _, bool compiled) in calls)
        {
            if (!compiled)
            {
                ((ExprEvalStep*)call)->d.func.fn_addr = new PGFunction((void*)s_guardedNextval);
            }
        }

        List<(nint Step, bool Single, bool Compiled)> draws = [];
        s_findingIdentities = true;
        try
        {
            FindCalls(query->planstate, draws, []);
        }
        finally
        {
            s_findingIdentities = false;
        }

        foreach ((nint draw, _, bool compiled) in draws)
        {
            if (!compiled && ReadDraw())
            {
                *(ExprEvalStep*)draw = WholeDraw(*(ExprEvalStep*)draw, ((ExprEvalStep*)draw)->d.nextvalueexpr.seqid);
            }
        }
    }

    // The pieces of memory that hold what the call an identity column's draw becomes needs, one for each sequence,
    // kept for the backend.
    private static readonly Dictionary<uint, nint> s_drawPieces = [];

    // The step an identity column's draw becomes as a call of nextval that runs as the draw does, with no check of
    // the role's rights, and goes through the fmgr hook, which sends a call it has no place for into the guard
    // whole. What the call needs is kept for the backend, one piece for each sequence: no two of a sequence's draws
    // run at once.
    private static unsafe ExprEvalStep WholeDraw(ExprEvalStep own, uint seqoid)
    {
        if (!s_drawPieces.TryGetValue(seqoid, out nint piece))
        {
            ExprEvalStep made = own;
            made.d.nextvalueexpr.seqid = seqoid;
            _ = HookDraw(&made, NativeGlobals.TopMemoryContext);
            piece = (nint)made.d.func.finfo;
            s_drawPieces[seqoid] = piece;
        }

        // The call is PostgreSQL's function that wraps a call in the fmgr hook, as GuardCall has a call it finds be.
        ExprEvalStep call = own;
        call.opcode = s_drawCode;
        call.d.func.finfo = (FmgrInfo*)piece;
        call.d.func.fcinfo_data = ((FunctionCallInfoBaseData*)((byte*)piece + sizeof(FmgrInfo) + DrawCacheSize));
        call.d.func.fn_addr = new PGFunction((void*)s_hookedCall);
        call.d.func.nargs = 1;
        return call;
    }

    /// <summary>
    /// Gets whether a statement running goes by what the guard kept of it, and so starts no expression as it runs
    /// that the guard would have to read.
    /// </summary>
    /// <param name="queryDesc">The statement.</param>
    /// <returns>Whether it does.</returns>
    internal static bool RunsSettled(nint queryDesc) => s_settledRunning.Count != 0 && s_settledRunning.Contains(queryDesc);

    // Has the guard's triggers on the tables a statement whose rows take their defaults writes skip its rows, or,
    // where the tables' own triggers may change a row, set each row's value against the value it drew.
    private static unsafe void SettleGuards(QueryDesc* query, bool takes, short verifies)
    {
        if (query->estate is null || query->estate->es_opened_result_relations is null)
        {
            return;
        }

        PlannedStmt* statement = query->plannedstmt;
        bool modifying = statement is not null && statement->hasModifyingCTE;
        bool upserts = query->operation == CmdType.CMD_INSERT && statement is not null && statement->planTree is not null &&
            statement->planTree->type == NodeTag.T_ModifyTable && ((ModifyTable*)statement->planTree)->onConflictAction == OnConflictAction.ONCONFLICT_UPDATE;

        // The rows of a statement whose table's triggers may change what a row drew are set against it: the node
        // that gives the rows, as it works one out, notes the value where the condition reads it.
        PlanState* rows = verifies == 0 || query->planstate is null ? null : query->planstate->lefttree;
        if (rows is not null && rows->ps_ProjInfo is not null && rows->ps_ProjInfo->pi_state.resultslot is not null &&
            verifies <= rows->ps_ProjInfo->pi_state.resultslot->tts_tupleDescriptor->natts)
        {
            TupleTableSlot* row = rows->ps_ProjInfo->pi_state.resultslot;
            s_noting = ((nint)(&rows->ps_ProjInfo->pi_state), (nint)(&row->tts_values[verifies - 1]), (nint)(&row->tts_isnull[verifies - 1]));
        }

        try
        {
            AttachGuards(query->estate, takes, modifying || query->operation is CmdType.CMD_INSERT or CmdType.CMD_MERGE,
                modifying || upserts || query->operation is CmdType.CMD_UPDATE or CmdType.CMD_MERGE);
        }
        finally
        {
            s_noting = default;
        }

        if (!takes && verifies == 0)
        {
            CheckRoutedRows(query->planstate, query->estate);
            for (int i = 0; modifying && query->estate->es_subplanstates is not null && i < query->estate->es_subplanstates->length; i++)
            {
                CheckRoutedRows((PlanState*)query->estate->es_subplanstates->elements[i].ptr_value, query->estate);
            }
        }
    }

    // Brackets the draws found among a statement's, or a COPY's, calls that go through the guard and name a managed
    // sequence, the last of each expression's first, as a bracket moves the steps after it.
    private static unsafe void BracketDraws(nint key, List<(nint Step, uint Seqoid, ExprEvalStep Own)> draws, Dictionary<nint, nint> states, bool identities = false)
    {
        s_drawsFound += draws.Count;
        if (draws.Count == 0 || s_fastSlot is null)
        {
            return;
        }

        draws.Sort(static (one, other) => other.Step.CompareTo(one.Step));
        foreach ((nint step, uint seqoid, ExprEvalStep own) in draws)
        {
            if (!states.TryGetValue(step, out nint state))
            {
                continue;
            }

            int index = (int)((ExprEvalStep*)step - ((ExprState*)state)->steps);
            nint bracket = BracketDraw((ExprState*)state, (ExprEvalStep*)step, seqoid, own);
            if (bracket == 0)
            {
                continue;
            }

            s_drawsBracketed.Add((state, index, seqoid, identities));

            if (!s_brackets.TryGetValue(key, out List<nint>? made))
            {
                s_brackets.Add(key, made = []);
            }

            made.Add(bracket);
        }
    }

    // Gives back the draws a statement's brackets took ahead and didn't take, as the statement ends, while the draws
    // left stand as they did when the brackets took them.
    private static unsafe void ReturnBrackets(nint key)
    {
        if (s_brackets.Count == 0 || !s_brackets.Remove(key, out List<nint>? made))
        {
            return;
        }

        foreach (nint held in made)
        {
            WorfDrawBracket* bracket = (WorfDrawBracket*)held;
            SettleBracket(bracket);
            s_freeBrackets.Push(held);
        }
    }

    // Counts the draws a bracket took of those it had taken ahead, and gives the rest back.
    private static unsafe void SettleBracket(WorfDrawBracket* bracket)
    {
        if (bracket->Given > 0 && bracket->Epoch == s_allowanceEpoch)
        {
            ref long left = ref CollectionsMarshal.GetValueRefOrNullRef(s_drawingUngated, bracket->Seqoid);
            if (!Unsafe.IsNullRef(ref left))
            {
                left += bracket->Budget;
            }

            ref (long Promised, long Drawn) promised = ref CollectionsMarshal.GetValueRefOrNullRef(s_promised, bracket->Seqoid);
            if (!Unsafe.IsNullRef(ref promised))
            {
                promised.Drawn += bracket->Given - bracket->Budget;
            }
        }

        bracket->Given = 0;
        bracket->Budget = 0;
    }

    // Whether a bracketed draw may go on at once: this backend holds no gate and runs no check, and counts its draw
    // under way before it looks for a switch, as a call that goes through the fmgr hook does.
    private static unsafe bool DrawsBracketed()
    {
        if (s_gating.Count != 0 || s_gates.Count != 0 || s_countingDown || s_namingGrantors || Worf.InOwnSql)
        {
            return false;
        }

        _ = Interlocked.Exchange(ref *s_fastSlot, 1);
        if (Volatile.Read(ref s_fastDraws[WorfFastDraws.Pending]) != 0 || Volatile.Read(ref s_fastDraws[WorfFastDraws.Changes]) != s_changesRead ||
            s_version < 0 || SequenceRanges.LocalVersion != s_localVersion)
        {
            Volatile.Write(ref *s_fastSlot, 0);
            return false;
        }

        return true;
    }

    // Takes draws ahead for a bracket from those its backend may take before it checks the sequence again, once
    // those it took before are counted, by the rule a call that goes through the fmgr hook counts each draw by.
    private static unsafe bool FillBracket(WorfDrawBracket* bracket)
    {
        SettleBracket(bracket);
        ref long left = ref CollectionsMarshal.GetValueRefOrNullRef(s_drawingUngated, bracket->Seqoid);
        if (Unsafe.IsNullRef(ref left) || left < 1)
        {
            return false;
        }

        long take = Math.Min(left, BracketBatch);
        ref (long Promised, long Drawn) promised = ref CollectionsMarshal.GetValueRefOrNullRef(s_promised, bracket->Seqoid);
        if (!Unsafe.IsNullRef(ref promised))
        {
            take = Math.Min(take, Math.Max(PromiseBatch, promised.Promised / 8) - promised.Drawn - 1);
        }

        if (take < 1)
        {
            return false;
        }

        left -= take;
        bracket->Given = take;
        bracket->Budget = take;
        bracket->Epoch = s_allowanceEpoch;
        return true;
    }

    // Runs as the step before a bracketed draw, within PostgreSQL's interpreter, where worf may call nothing of
    // PostgreSQL's and let no exception out. The draw after it is PostgreSQL's own where it may go on at once, and
    // otherwise the call that goes through the guard.
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void DrawStep(ExprState* state, ExprEvalStep* step, ExprContext* context)
    {
        WorfDrawBracket* bracket = (WorfDrawBracket*)step->d.cparam.paramarg;
        bool direct = false;
        try
        {
            direct = ((bracket->Budget > 0 && bracket->Epoch == s_allowanceEpoch) || FillBracket(bracket)) && DrawsBracketed();
            if (direct)
            {
                bracket->Budget--;
            }
        }
        catch (Exception failure) when (failure is NullReferenceException or InvalidOperationException or ArgumentException or
            IndexOutOfRangeException or KeyNotFoundException)
        {
            // Nothing above throws unless worf's own state is wrong. The next statement that starts reports it.
            Executor.NoteHookFailure(failure);
            direct = false;
        }

        if (direct != bracket->Direct)
        {
            step[1] = direct ? bracket->Own : bracket->Guarded;
            bracket->Direct = direct;
        }
    }

    // Deals with the expressions that started a call in a subtransaction that aborts before PL/pgSQL told of a
    // statement. An expression's state outlives the subtransaction, and PL/pgSQL runs it again without starting it,
    // so its calls go through the guard from here on, unless a DO block that the abort ended ran it, which took its
    // expressions with it: PL/pgSQL's error contexts, by now those of the functions still running, tell.
    private static unsafe void ForgetSitesOf(uint aborted)
    {
        if (s_startedSites.Count == 0)
        {
            return;
        }

        PLpgSQL_plugin* plugin = s_plpgsqlPlugin is null ? null : (PLpgSQL_plugin*)*s_plpgsqlPlugin;
        void* callback = plugin is null || plugin->error_callback.IsNull ? null : plugin->error_callback.DangerousGetAddress();
        for (int i = s_startedSites.Count - 1; i >= 0; i--)
        {
            (nint started, uint subxact, nint estate, bool inline) = s_startedSites[i];
            if (subxact < aborted)
            {
                continue;
            }

            bool running = false;
            for (ErrorContextCallback* context = NativeGlobals.error_context_stack; inline && callback is not null && context is not null && !running;
                context = context->previous)
            {
                running = context->callback.DangerousGetAddress() == callback && (nint)context->arg == estate;
            }

            if (!inline || running)
            {
                _ = GuardSite(started);
            }

            s_startedSites.RemoveAt(i);
        }

        WatchStatements();
    }

    // Whether a step of an expression calls a function.
    private static bool CallsFunction(ExprEvalOp op)
        => op is ExprEvalOp.EEOP_FUNCEXPR or ExprEvalOp.EEOP_FUNCEXPR_STRICT or ExprEvalOp.EEOP_FUNCEXPR_FUSAGE or ExprEvalOp.EEOP_FUNCEXPR_STRICT_FUSAGE
#if !(ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17)
            or ExprEvalOp.EEOP_FUNCEXPR_STRICT_1 or ExprEvalOp.EEOP_FUNCEXPR_STRICT_2
#endif
            ;

    // The sequence a call of nextval names with a constant, or zero for one that names it as it runs.
    private static unsafe uint NamedByCall(Node* call)
    {
        List* args = call is null ? null
            : call->type == NodeTag.T_FuncExpr ? ((FuncExpr*)call)->args
            : call->type == NodeTag.T_OpExpr ? ((OpExpr*)call)->args
            : null;
        Node* arg = args is null || args->length != 1 ? null : (Node*)args->elements[0].ptr_value;
        return arg is not null && arg->type == NodeTag.T_Const && !((Const*)arg)->constisnull ? (uint)((Const*)arg)->constvalue : 0;
    }

    // This backend's number for the transaction it runs, which PL/pgSQL marks the expressions it started in it with.
    private static unsafe uint Lxid()
#if ANKUS_PG15 || ANKUS_PG16
        => NativeGlobals.MyProc->lxid;
#else
        => NativeGlobals.MyProc->vxid.lxid;
#endif

    // The numbers of pg_proc's columns prosecdef, prosrc and proconfig, found once for the backend.
    private static (short Definer, short Source, short Settings) s_functionColumns;

    // Reads what worf asks of a function from PostgreSQL's cache of functions, which a statement that calls it has
    // filled: its language and schema, whether it runs as its owner, whether it has settings of its own, and its
    // source. Null for a function that is gone.
    private static unsafe (uint Language, uint Schema, bool Definer, bool Settings, string Source)? FunctionRow(uint function)
    {
        // pg_proc, and its columns pronamespace and prolang, which every supported version numbers so.
        const uint Procedures = 1255;
        const short Schema = 3;
        const short Language = 5;
#if ANKUS_PG19
        const SysCacheIdentifier Functions = SysCacheIdentifier.PROCOID;
#else
        const int Functions = ProcedureCacheId;
#endif
        if (s_functionColumns.Source == 0)
        {
            s_functionColumns = (NativeMethods.get_attnum(Procedures, Worf.PStrDup("prosecdef")), NativeMethods.get_attnum(Procedures, Worf.PStrDup("prosrc")),
                NativeMethods.get_attnum(Procedures, Worf.PStrDup("proconfig")));
        }

        HeapTupleData* tuple = NativeMethods.SearchSysCache1(Functions, function);
        if (tuple is null)
        {
            return null;
        }

        try
        {
            bool isNull;
            bool none;
            uint schema = (uint)NativeMethods.SysCacheGetAttr(Functions, tuple, Schema, &isNull);
            uint language = (uint)NativeMethods.SysCacheGetAttr(Functions, tuple, Language, &isNull);
            bool definer = (byte)NativeMethods.SysCacheGetAttr(Functions, tuple, s_functionColumns.Definer, &isNull) != 0;
            _ = NativeMethods.SysCacheGetAttr(Functions, tuple, s_functionColumns.Settings, &none);
            ulong source = NativeMethods.SysCacheGetAttr(Functions, tuple, s_functionColumns.Source, &isNull);
            return (language, schema, definer, !none, isNull ? "" : Worf.FromServer(NativeMethods.text_to_cstring((varlena*)source)));
        }
        finally
        {
            NativeMethods.ReleaseSysCache(tuple);
        }
    }

    // Finds PostgreSQL's function that wraps a call in the fmgr hook, once for the backend. PostgreSQL has a function
    // that isn't built in go through it where the hook check asks for it, so the check asks for it for PL/pgSQL's call
    // handler meanwhile. Calls of nextval go through it only where the fmgr hook is worf's own and counts them without
    // a call into PostgreSQL, and where nextval has no settings or owner to run as, which PostgreSQL ignores for a
    // built-in function and that function wouldn't.
    private static unsafe void ReadHookedCall()
    {
        if (s_hookedCallRead)
        {
            return;
        }

        if (Executor.CountsDraws && FunctionRow(NextvalOid) is (_, _, false, false, _))
        {
            List* name = NativeMethods.lappend(null, NativeMethods.makeString(Worf.PStrDup("pg_catalog")));
            name = NativeMethods.lappend(name, NativeMethods.makeString(Worf.PStrDup("plpgsql_call_handler")));
            uint handler = NativeMethods.LookupFuncName(name, 0, null, true);
            FmgrInfo own = default;
            FmgrInfo hooked = default;
            if (handler != 0)
            {
                NativeMethods.fmgr_info(handler, &own);
                ReadingHookedCall = true;
                try
                {
                    NativeMethods.fmgr_info(handler, &hooked);
                }
                finally
                {
                    ReadingHookedCall = false;
                }
            }

            if (!hooked.fn_addr.IsNull && hooked.fn_addr.DangerousGetAddress() != own.fn_addr.DangerousGetAddress())
            {
                // The count is the backend's own, which a backend before it in its place may have left set as it failed.
                long number = ((nint)NativeGlobals.MyProc - (nint)NativeGlobals.ProcGlobal->allProcs) / sizeof(PGPROC);
                s_fastSlot = number >= 0 && number < WorfFastDraws.Slots ? Shared() + ((number + 1) * WorfFastDraws.SlotLongs) : null;
                if (s_fastSlot is not null)
                {
                    Volatile.Write(ref *s_fastSlot, 0);
                }

                s_hookedCall = (nint)hooked.fn_addr.DangerousGetAddress();
            }
        }

        s_hookedCallRead = true;
    }

    // What shared memory holds for the draws that hold no gate, where this backend has it, found once.
    private static unsafe long* Shared()
    {
        if (s_fastDraws is null)
        {
            s_fastDraws = (long*)FastDraws.Read(static (in shared) => (nint)Unsafe.AsPointer(ref Unsafe.AsRef(in shared)));
        }

        return s_fastDraws;
    }

    /// <summary>
    /// Tells the draws that hold no gate of a change to the blocks, once the counter every backend reads has moved.
    /// </summary>
    internal static unsafe void TellOfChange() => _ = Interlocked.Increment(ref Shared()[WorfFastDraws.Changes]);

    // Counts a switch as pending, or no more, for the statements that hold gates and for the draws that hold none.
    private static unsafe void CountPending(long change)
    {
        _ = PgAtomic.Add(SequenceRanges.SwitchesPending, change);
        _ = Interlocked.Add(ref Shared()[WorfFastDraws.Pending], change);
    }

    /// <summary>
    /// Gets whether the guard is asking PostgreSQL where its function that wraps a call in the fmgr hook is, for
    /// which the hook check answers yes.
    /// </summary>
    internal static bool ReadingHookedCall { get; private set; }

    /// <summary>
    /// Runs in the fmgr hook as a call of nextval that goes through it starts, within PostgreSQL's own call, where
    /// worf may call nothing of PostgreSQL's: the draw goes on at once where this backend has draws of the sequence
    /// left to take before it checks it again, and no switch has started or committed since it last read the blocks.
    /// It then counts among the draws under way, which a switch waits for before it moves a sequence, in place of
    /// holding the sequence's gate. Any other call goes into <see cref="GuardedNextval"/> instead of nextval.
    /// </summary>
    /// <param name="flinfo">The function about to be called.</param>
    /// <param name="site">The call's own value in the hook, which keeps its sequence.</param>
    internal static unsafe void DrawStarting(FmgrInfo* flinfo, ulong* site)
    {
        if (!DrawsAtOnce(flinfo, site))
        {
            s_handedOff = (nint)flinfo;
            flinfo->fn_addr = new PGFunction((void*)s_guardedNextval);
        }
    }

    /// <summary>
    /// Runs in the fmgr hook as a call of nextval that goes through it ends or fails: its draw is under way no more.
    /// </summary>
    /// <param name="flinfo">The function called.</param>
    internal static unsafe void DrawEnded(FmgrInfo* flinfo)
    {
        LeaveFastDraw();
        if (s_handedOff == (nint)flinfo)
        {
            s_handedOff = 0;
            flinfo->fn_addr = new PGFunction((void*)s_nextval);
        }
    }

    // Whether a call of nextval may draw without a call into PostgreSQL, and counts its draw if so.
    private static unsafe bool DrawsAtOnce(FmgrInfo* flinfo, ulong* site)
    {
        // A check under way, and worf's own statements, count no draws, as before.
        if (s_countingDown || s_namingGrantors || Worf.InOwnSql)
        {
            return true;
        }

        // A call finds its place by its place in the plan, once: PostgreSQL keeps what it looked up for the call, which
        // this hook is given, where the call's own lookup points, so a place whose lookup points elsewhere is another
        // run's of the same plan, and the call goes into GuardedNextval, which reads its own arguments.
        if (*site >> 32 != s_siteGeneration)
        {
            int place = s_siteOf.TryGetValue((nint)flinfo->fn_expr, out int found) && ((FmgrInfo*)s_sites[found].Function)->fn_extra == flinfo ? found + 1 : 0;
            *site = ((ulong)s_siteGeneration << 32) | (uint)place;
        }

        int index = (int)(uint)*site - 1;
        if (s_fastSlot is null)
        {
            return false;
        }

        // A call with no place that PostgreSQL compiled to go through the hook is one the guard didn't find in the
        // plan, or gave no place, as one of a sequence that isn't managed: its expression still tells the sequence
        // it names, and one that names none goes into GuardedNextval.
        uint seqoid;
        nint arguments = 0;
        bool placed = index >= 0 && index < s_sites.Count;
        if (placed)
        {
            (seqoid, arguments, _) = s_sites[index];
        }
        else if (!s_compiledCalls || (seqoid = NamedByCall(flinfo->fn_expr)) == 0)
        {
            return false;
        }

        // A call that names its sequence as it runs has worked its argument out by now. One of a sequence that isn't
        // managed goes on as it is, where the blocks this backend read still hold.
        if (seqoid == 0)
        {
            if (arguments == 0)
            {
                return false;
            }

            seqoid = (uint)*(ulong*)arguments;
            if (Volatile.Read(ref s_fastDraws[WorfFastDraws.Changes]) != s_changesRead || s_version < 0 || SequenceRanges.LocalVersion != s_localVersion)
            {
                return false;
            }

            if (seqoid == s_snowflake || !s_sequences.ContainsKey(seqoid))
            {
                return true;
            }
        }

        // A statement that writes and holds the sequence's gate checks its rows' draws itself.
        for (int i = 0; i < s_gating.Count; i++)
        {
            if (s_gating[i].Sequences.Contains(seqoid))
            {
                return true;
            }
        }

        ref long left = ref CollectionsMarshal.GetValueRefOrNullRef(s_drawingUngated, seqoid);
        if (Unsafe.IsNullRef(ref left))
        {
            // A compiled call with no place may name a sequence that isn't managed, which goes on as it is.
            return !placed && Volatile.Read(ref s_fastDraws[WorfFastDraws.Changes]) == s_changesRead && s_version >= 0 &&
                SequenceRanges.LocalVersion == s_localVersion && (seqoid == s_snowflake || !s_sequences.ContainsKey(seqoid));
        }

        if (left < 1)
        {
            return false;
        }

        // The draws taken of the count are told to the shared table once there are a few, which takes its lock.
        ref (long Promised, long Drawn) promised = ref CollectionsMarshal.GetValueRefOrNullRef(s_promised, seqoid);
        bool counted = !Unsafe.IsNullRef(ref promised);
        if (counted && promised.Drawn + 1 >= Math.Max(PromiseBatch, promised.Promised / 8))
        {
            return false;
        }

        // A switch counts itself as pending before it looks for draws under way, and this draw counts itself under
        // way before it looks for a switch, so one of them sees the other. A switch that has committed has told of
        // its change first.
        if (s_gates.Count == 0 || !s_gates.ContainsKey(seqoid))
        {
            _ = Interlocked.Increment(ref *s_fastSlot);
            if (Volatile.Read(ref s_fastDraws[WorfFastDraws.Pending]) != 0 || Volatile.Read(ref s_fastDraws[WorfFastDraws.Changes]) != s_changesRead ||
                s_version < 0 || SequenceRanges.LocalVersion != s_localVersion)
            {
                _ = Interlocked.Decrement(ref *s_fastSlot);
                return false;
            }

            s_drawingFast = true;
        }

        left--;
        if (counted)
        {
            promised.Drawn++;
        }

        return true;
    }

    // Counts this backend's draw under way no more.
    private static unsafe void LeaveFastDraw()
    {
        // The count is this backend's alone, and a draw a bracket's step counted, which failed before the step after
        // it ran, left it set without a note here.
        if (s_fastSlot is not null && (s_drawingFast || Volatile.Read(ref *s_fastSlot) != 0))
        {
            s_drawingFast = false;
            Volatile.Write(ref *s_fastSlot, 0);
        }
    }

    // Draws for a call of nextval in a PL/pgSQL expression that the fmgr hook couldn't count at once, or that doesn't
    // go through it. The call is a unit, as a statement's row is: it takes its sequence's gate, counts the draw down,
    // waiting for the switch where the block is short, draws and lets the gate go. A sequence the transaction hasn't
    // drawn from without its gate yet is checked first, as a call that starts is.
    private static unsafe ulong GuardedNextval(FunctionCallInfoBaseData* fcinfo)
    {
        if (s_handedOff != 0 && s_handedOff == (nint)fcinfo->flinfo)
        {
            s_handedOff = 0;
            fcinfo->flinfo->fn_addr = new PGFunction((void*)s_nextval);
        }

        ulong* args = (ulong*)((byte*)fcinfo + sizeof(FunctionCallInfoBaseData));
        uint seqoid = (uint)args[0];
        bool quiet = NativeGlobals.creating_extension || NativeGlobals.XactReadOnly;
        if (s_countingDown || s_namingGrantors || Worf.InOwnSql || quiet)
        {
            return NativeMethods.nextval_oid(fcinfo);
        }

        // The blocks are read again after a change, which the hook leaves to this call.
        s_namingGrantors = true;
        try
        {
            Reload();
        }
        finally
        {
            s_namingGrantors = false;
        }

        if (!s_sequences.ContainsKey(seqoid) || StatementGated(seqoid) || seqoid == Worf.CatalogOid("snowflake_node"))
        {
            return NativeMethods.nextval_oid(fcinfo);
        }

        EnterScope(RowScope, (nint)fcinfo);
        bool failed = true;
        try
        {
            if (!s_drawingUngated.ContainsKey(seqoid))
            {
                List<uint> starting = [seqoid];
                CountFunction(starting);
                GateUnit(seqoid);
                HearOfSwitches();
                WaitWhileShort(seqoid);
                DrawUngated(starting);
            }

            CountDownDraws(1, [seqoid], single: s_singleCalls.Count > 0 && s_singleCalls.Contains((nint)fcinfo) && !DrawsAgainWithin(seqoid));
            ulong value = NativeMethods.nextval_oid(fcinfo);
            failed = false;
            return value;
        }
        finally
        {
            LeaveScope(RowScope, (nint)fcinfo, failed: failed);
        }
    }

    // Has a running statement's plan nodes count down before each row they produce, for the calls of nextval the CHECK
    // constraints and trigger conditions of the tables it writes make for each row, at least one, from the first call
    // that starts as it runs on: nodes that have already produced a row run their own function, which the count takes the
    // place of too.
    private static unsafe void CountRunningRows(nint queryDesc, nint valuesRow = 0)
    {
        QueryDesc* query = (QueryDesc*)queryDesc;
        if (query->plannedstmt is null || query->planstate is null)
        {
            return;
        }

        // For a VALUES list's row, whose calls each start checks, the plan's top node alone counts, the rows the
        // client reads; for any other call, every node does, those under a top node that counted alone too.
        List<nint> nodes = [];
        bool under = valuesRow == 0 && s_runTop.Remove(queryDesc);
        CountRows(query->planstate, nodes, RunningRowCalls(queryDesc, valuesRow), running: true, under: under, alone: valuesRow != 0);
        foreach (nint node in nodes)
        {
            s_runRows[node] = queryDesc;
        }

        if (valuesRow != 0)
        {
            _ = s_runTop.Add(queryDesc);
        }

        if (nodes.Count > 0)
        {
            // A cursor's nodes, patched as a FETCH runs it in a savepoint, are its own as long as it is, so they're kept
            // by the subtransaction it was made in, which a rollback to the savepoint leaves, rather than the FETCH's.
            PortalData* portal = NativeGlobals.ActivePortal;
            RegisterCounted();
            if (s_countedNodes.TryGetValue(queryDesc, out (uint Subxact, List<nint> Nodes) counted))
            {
                nodes.AddRange(counted.Nodes);
            }

            s_countedNodes[queryDesc] = (portal is not null && portal->queryDesc == query ? portal->createSubid : NativeMethods.GetCurrentSubTransactionId(), nodes);
        }
    }

    // The calls of nextval a running statement's row may make as it starts them: those of the CHECK constraints and
    // trigger conditions of the tables it writes, or of a row of a VALUES list, at least one.
    private static unsafe int RunningRowCalls(nint queryDesc, nint valuesRow = 0)
    {
        QueryDesc* query = (QueryDesc*)queryDesc;
        if (query->plannedstmt is null || query->planstate is null)
        {
            return 1;
        }

        // A call that starts in a VALUES list's row is one of that row's, which alone is read for its calls.
        int row = 0;
        if (valuesRow != 0)
        {
            ValuesScanState* values = (ValuesScanState*)valuesRow;
            List* rows = values->ss.ps.plan is null ? null : ((ValuesScan*)values->ss.ps.plan)->values_lists;
            row = rows is not null && values->curr_idx >= 0 && values->curr_idx < rows->length
                ? Draws(PlanText((Node*)rows->elements[values->curr_idx].ptr_value)).Calls : ValuesRowCalls(query->planstate);
        }
        else if (!s_valuesRows.TryGetValue(queryDesc, out (nint Memory, nint Node)[]? lists) || lists.Length == 0)
        {
            // A VALUES list's calls count themselves as they start, so the statement's other rows count none for them.
            row = ValuesRowCalls(query->planstate);
        }

        long[] written = [.. Writing(query->plannedstmt).SelectMany(write => write.Written)];
        return Math.Max(Math.Max(written.Length > 0 ? Draws(WrittenChecks(written)).Calls : 0, row), 1);
    }

    // The VALUES list of a running statement whose row's expressions PostgreSQL is building, as it does in the row's own
    // memory, or zero.
    private static unsafe nint ValuesRow(QueryDesc* query)
    {
        if (!s_valuesRows.TryGetValue((nint)query, out (nint Memory, nint Node)[]? rows))
        {
            List<(nint, nint)> lists = [];
            ValuesRows(query->planstate, lists, []);
            rows = [.. lists];
            s_valuesRows[(nint)query] = rows;
        }

        if (rows.Length == 0)
        {
            return 0;
        }

        nint memory = (nint)NativeGlobals.CurrentMemoryContext;
        foreach ((nint row, nint node) in rows)
        {
            if (row == memory)
            {
                return node;
            }
        }

        return 0;
    }

    // Finds a plan's VALUES lists, through the nodes ValuesRowCalls reaches.
    private static unsafe void ValuesRows(PlanState* node, List<(nint Memory, nint Node)> lists, HashSet<nint> seen)
    {
        if (node is null || !seen.Add((nint)node))
        {
            return;
        }

        ValuesRows(node->lefttree, lists, seen);
        ValuesRows(node->righttree, lists, seen);
        foreach (List* subplans in new[] { node->initPlan, node->subPlan })
        {
            for (int i = 0; subplans is not null && i < subplans->length; i++)
            {
                ValuesRows(((SubPlanState*)subplans->elements[i].ptr_value)->planstate, lists, seen);
            }
        }

        switch (node->type)
        {
            case NodeTag.T_ValuesScanState when ((ValuesScanState*)node)->rowcontext is not null:
                lists.Add(((nint)((ValuesScanState*)node)->rowcontext->ecxt_per_tuple_memory, (nint)node));
                break;
            case NodeTag.T_SubqueryScanState:
                ValuesRows(((SubqueryScanState*)node)->subplan, lists, seen);
                break;
            case NodeTag.T_AppendState:
                for (int i = 0; i < ((AppendState*)node)->as_nplans; i++)
                {
                    ValuesRows(((AppendState*)node)->appendplans[i], lists, seen);
                }

                break;
            case NodeTag.T_MergeAppendState:
                for (int i = 0; i < ((MergeAppendState*)node)->ms_nplans; i++)
                {
                    ValuesRows(((MergeAppendState*)node)->mergeplans[i], lists, seen);
                }

                break;
            case NodeTag.T_CteScanState:
                ValuesRows(((CteScanState*)node)->cteplanstate, lists, seen);
                break;
            default:
                break;
        }
    }

    // Whether a running statement's nodes are to start counting rows down for a call that starts as it runs: all of
    // them where none does yet, or only the top one does, and for a VALUES list's row the top one alone, where none
    // does and the client reads the rows.
    private static bool RowsAreToCount(nint queryDesc, nint valuesRow)
    {
        bool counting = s_countedNodes.ContainsKey(queryDesc);
        return valuesRow == 0 ? !counting || s_runTop.Contains(queryDesc) : !counting && !RunKeepsGates(queryDesc);
    }

    // Whether a statement's run keeps its rows' gates from row to row, as one whose rows no client reads does.
    private static bool RunKeepsGates(nint queryDesc)
    {
        for (int i = s_scopes.Count - 1; i >= 0; i--)
        {
            if (s_scopes[i].Kind == RunScope && s_scopes[i].Key == queryDesc)
            {
                return s_scopes[i].Owner != 0;
            }
        }

        return false;
    }

    // The most calls of nextval a row of a VALUES list in a plan makes, which it starts all as the row is reached, before
    // it draws any: in the plan's nodes, those under them, their initPlans and subplans, as a CTE's, and the plans an
    // Append, a MergeAppend, a subquery scan or a CTE scan runs, each once.
    private static unsafe int ValuesRowCalls(PlanState* node) => ValuesRowCalls(node, []);

    private static unsafe int ValuesRowCalls(PlanState* node, HashSet<nint> seen)
    {
        if (node is null || !seen.Add((nint)node))
        {
            return 0;
        }

        int most = Math.Max(ValuesRowCalls(node->lefttree, seen), ValuesRowCalls(node->righttree, seen));
        foreach (List* subplans in new[] { node->initPlan, node->subPlan })
        {
            for (int i = 0; subplans is not null && i < subplans->length; i++)
            {
                most = Math.Max(most, ValuesRowCalls(((SubPlanState*)subplans->elements[i].ptr_value)->planstate, seen));
            }
        }

        switch (node->type)
        {
            case NodeTag.T_ValuesScanState when node->plan is not null:
                List* rows = ((ValuesScan*)node->plan)->values_lists;
                for (int i = 0; rows is not null && i < rows->length; i++)
                {
                    most = Math.Max(most, Draws(PlanText((Node*)rows->elements[i].ptr_value)).Calls);
                }

                break;
            case NodeTag.T_SubqueryScanState:
                most = Math.Max(most, ValuesRowCalls(((SubqueryScanState*)node)->subplan, seen));
                break;
            case NodeTag.T_AppendState:
                for (int i = 0; i < ((AppendState*)node)->as_nplans; i++)
                {
                    most = Math.Max(most, ValuesRowCalls(((AppendState*)node)->appendplans[i], seen));
                }

                break;
            case NodeTag.T_MergeAppendState:
                for (int i = 0; i < ((MergeAppendState*)node)->ms_nplans; i++)
                {
                    most = Math.Max(most, ValuesRowCalls(((MergeAppendState*)node)->mergeplans[i], seen));
                }

                break;
            case NodeTag.T_CteScanState:
                most = Math.Max(most, ValuesRowCalls(((CteScanState*)node)->cteplanstate, seen));
                break;
            default:
                break;
        }

        return most;
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
                _ = s_gatedRows.Remove(node);
                _ = s_runRows.Remove(node);
            }
        }
    }

    private static void RegisterCounted() => RegisterEnd();

    // Whether what the guard does as a transaction is prepared is registered for this transaction.
    private static bool s_prepareRegistered;

    /// <summary>
    /// Registers, as PREPARE TRANSACTION starts, what the guard does as a transaction that drew is prepared: it
    /// lets its gates go before, and forgets its statements after, as it does at a commit.
    /// </summary>
    internal static void RegisterPrepare()
    {
        if (!s_endRegistered || s_prepareRegistered)
        {
            return;
        }

        s_prepareRegistered = true;
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.PrePrepare, static () => _ = LetGates());
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Prepare, static () => ForgetStatements(false));
    }

    // Has the transaction forget, as it ends, what the guard notes of the statements that draw in it, and a
    // subtransaction that aborts what it notes of that one's: the statements counted among their sequences' drawers, the
    // draws they may take without a check, the plan nodes that count down, and the gates. The transaction takes the
    // callbacks on once, with its first such statement.
    private static void RegisterEnd()
    {
        if (s_endRegistered)
        {
            return;
        }

        // What a transaction's preparing takes is registered as PREPARE TRANSACTION comes, with RegisterPrepare:
        // each registration costs every statement that draws, and few transactions are prepared.
        s_endRegistered = true;
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, static () => ForgetStatements(false));
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, static () => ForgetStatements(true));
        _ = PgTransaction.RegisterSubtransactionCallback(PgSubtransactionEvent.Commit, static (subxact, parent) => PassGatesToParent(subxact.Value, parent.Value));
        _ = PgTransaction.RegisterSubtransactionCallback(PgSubtransactionEvent.Abort, static (aborted, parent) =>
        {
            ForgetGatesOf(aborted.Value);
            if (s_routing.RemoveAll(routed => routed.Subxact >= aborted.Value) > 0)
            {
                s_routedMarked.Clear();
            }

            foreach (nint queryDesc in s_countedNodes.Where(entry => entry.Value.Subxact >= aborted.Value).Select(entry => entry.Key).ToList())
            {
                ForgetCounted(queryDesc);
            }

            foreach ((int, nint, uint, uint Seqoid) drawer in s_ungatedDrawers.Where(drawer => drawer.Subxact >= aborted.Value).ToList())
            {
                CountDrawer(drawer.Seqoid, -1);
                _ = s_ungatedDrawers.Remove(drawer);
            }
        });
    }

    private static void ForgetStatements(bool aborted)
    {
        s_endRegistered = false;
        s_prepareRegistered = false;
        s_routing.Clear();
        s_routedMarked.Clear();
        ForgetUngatedDrawers();
        ForgetUngated();

        // No statement waits for a switch once its transaction has ended.
        foreach ((uint seqoid, (int waiting, long lo)) in s_waitingFor.ToList())
        {
            CountDrawer(seqoid, 0, -waiting, waitingFor: lo);
            _ = s_waitingFor.Remove(seqoid);
        }

        // The statements of a transaction that aborts end without ExecutorEnd.
        if (aborted)
        {
            s_countedNodes.Clear();
            s_rowFunctions.Clear();
            s_gatedRows.Clear();
            s_runRows.Clear();
            s_runStarted.Clear();
            s_runFound.Clear();
            s_valuesRows.Clear();
            s_runTop.Clear();
            s_valuesRow = default;
        }

        ForgetGates();
    }

    // Notes the managed sequences a statement draws from without their switch gates, after their first check, until the
    // transaction ends.
    private static void DrawUngated(List<uint> sequences)
    {
        if (sequences.Count == 0)
        {
            return;
        }

        RegisterEnd();
        foreach (uint seqoid in sequences)
        {
            Share(seqoid);
            if (Position(seqoid) is long position)
            {
                s_unnamedMarks[seqoid] = position;
            }
        }
    }

    // Notes the managed sequences a statement's plan names as it starts, which the statement draws from without their
    // switch gates, for its first row to check: the plan starts its calls before the statement runs, as a cursor's does
    // before its first FETCH, and a check then would hold values back, and a gate, while the session idles.
    private static void DeferUngated(List<uint> sequences)
    {
        if (sequences.Count == 0)
        {
            return;
        }

        RegisterEnd();
        foreach (uint seqoid in sequences)
        {
            _ = s_drawingUngated.TryAdd(seqoid, 0);
        }
    }

    // Takes, as a call of nextval starts within a unit, the gates of the sequences it may draw from: for a call a
    // PL/pgSQL statement's own expression makes, which draws straight after, from now until PL/pgSQL next tells of a
    // statement of the function; for one a statement starts as it runs, the row it runs, or the run. A call that starts
    // otherwise, as one a utility statement makes, takes none.
    private static unsafe void GateStarting(bool plpgsql, nint estate, WorfStartingStatement? starting, List<uint> sequences)
    {
        if (plpgsql)
        {
            PLpgSQL_stmt* statement = estate == 0 ? null : ((PLpgSQL_execstate*)estate)->err_stmt;
            if (!TellsOfStatements() || statement is null)
            {
                return;
            }

            if (s_scopes.Count == 0 || s_scopes[^1] is not { Kind: DrawScope } top || top.Key != (nint)statement || top.Owner != estate)
            {
                EnterScope(DrawScope, (nint)statement, estate);
            }
        }
        else if (starting is not { Running: not 0 })
        {
            return;
        }

        foreach (uint seqoid in sequences.Order())
        {
            GateUnit(seqoid);
        }

        HearOfSwitches();
    }

    private static void ForgetUngated()
    {
        foreach (uint seqoid in s_promised.Keys.ToList())
        {
            Promise(seqoid, 0);
        }

        s_shareRuns.Clear();
        s_allowanceEpoch++;
        s_drawingUngated.Clear();
        s_sharesTaken.Clear();
        s_unnamedMarks.Clear();
    }

    // Counts a row, statement or call down, by the draws it may take, for each sequence drawn from without its gate that
    // it names, and, when it names none, or a call of it names none, for each other as far as it's seen to have drawn
    // from it since, or at once when its block has run out, as any draw from it would fail, and checks it for the
    // switch, as before its first draw, once its count runs out, which half the values left before the block's last
    // twentieth, in the sequence's steps, sets, as other sessions may draw too.
    private static void CountDownDraws(long draws = 1, IReadOnlyCollection<uint>? only = null, bool gated = false, bool unnamed = false,
        bool gate = true, bool single = false)
    {
        if (s_drawingUngated.Count == 0 || s_countingDown || s_namingGrantors || Worf.InOwnSql)
        {
            return;
        }

        s_countingDown = true;
        try
        {
            // The unit takes the gates of the sequences it may draw from, in their order, so that a switch beside it
            // moves none of them between its check and its draws. One that holds them already, as a row of many under
            // one that gathers them does, lets them go while a switch waits for them, and takes them again once the
            // switch has committed.
            int held = s_gates.Count;
            if (gate && s_scopes.Count > 0)
            {
                if (s_drawingUngated.Count == 1)
                {
                    uint seqoid = OnlyUngated();
                    if (only is null || unnamed || only.Contains(seqoid))
                    {
                        GateUnit(seqoid);
                    }
                }
                else
                {
                    foreach (uint seqoid in s_drawingUngated.Keys.Where(seqoid => only is null || unnamed || only.Contains(seqoid)).Order().ToList())
                    {
                        GateUnit(seqoid);
                    }
                }
            }

            // A unit that has just taken its gates holds up no switch yet. One that held them all before, as a row under
            // the row that took them, or a PL/pgSQL statement that keeps the one before's, looks for a waiting switch
            // as it starts, unless one looked a moment ago, as the look reads shared memory; and one with none, which
            // nothing keeps a switch from moving beside, always looks.
            bool took = s_gates.Count != held;
            bool looked = s_gates.Count == 0;
            if (!looked && !took)
            {
                long now = Stopwatch.GetTimestamp();
                looked = now - s_lookedAt >= s_lookEvery;
                s_lookedAt = looked ? now : s_lookedAt;
            }

            bool switching = looked && SequenceRanges.SwitchesPending.Value != 0;
            if (switching && s_gates.Count > 0)
            {
                _ = PassGates();
            }

            // A statement that draws row after row holds its sequences' locks, so PostgreSQL reads no bounds again as it
            // goes: once another session has switched a sequence, the next row reads the new block's from the catalog,
            // rather than run past the old block's end. No switch of a sequence commits while its gate is held, so a
            // unit that has held its gates since the last one read hears of the others' with its next look.
            if ((took || looked) && (SequenceRanges.Version.Value != s_version || SequenceRanges.LocalVersion != s_localVersion))
            {
                Refresh();
                ForgetBounds();
                Reload();

                // A switch beside the statement moves the sequence to its new block just before it commits, so until its
                // commit shows, the sequence stands past the bounds this backend reads, and a draw would fail; the row
                // waits for the commit, a moment.
                foreach (uint seqoid in s_drawingUngated.Keys.ToList())
                {
                    AwaitMovedBounds(seqoid);
                }
            }

            // So does a row while the switch is under way, as it may have moved the sequence already.
            else if (switching)
            {
                foreach (uint seqoid in s_drawingUngated.Keys.Where(seqoid => Drawer(seqoid).Switching > 0).ToList())
                {
                    AwaitMovedBounds(seqoid);
                }
            }

            if (s_drawingUngated.Count == 1)
            {
                CountDown(OnlyUngated(), draws, only, gated, unnamed, single);
                return;
            }

            foreach (uint seqoid in s_drawingUngated.Keys.ToList())
            {
                CountDown(seqoid, draws, only, gated, unnamed, single);
            }
        }
        finally
        {
            s_countingDown = false;
        }
    }

    // The one sequence drawn from without its gate, read without a list of them all, as every row reads it.
    private static uint OnlyUngated()
    {
        using Dictionary<uint, long>.KeyCollection.Enumerator keys = s_drawingUngated.Keys.GetEnumerator();
        return keys.MoveNext() ? keys.Current : 0;
    }

    // Counts one sequence down for a row, statement or call, as CountDownDraws does each.
    private static void CountDown(uint seqoid, long draws, IReadOnlyCollection<uint>? only, bool gated, bool unnamed, bool single)
    {
        long count = draws;
        bool runOut = false;
        if (only is null || !only.Contains(seqoid))
        {
            if (only is not null && !unnamed)
            {
                return;
            }

            count = SeenDrawn(seqoid, out long? seen);
            runOut = RunOut(seqoid, seen, draws);
            if (count == 0 && !runOut)
            {
                return;
            }
        }

        if (!runOut && s_drawingUngated[seqoid] >= count)
        {
            s_drawingUngated[seqoid] -= count;

            // A count taken while fewer statements drew from the sequence than do now holds back more of the block
            // than its share: a statement that started alone counts half the block, and those that start beside it
            // would find the rest short long before the block is, and wait for a switch off a block with values
            // left. It is taken again, for the statements that draw now, as the draws taken of it are told.
            if (Drew(seqoid, count) && s_sharedAmong.TryGetValue(seqoid, out int among) && Drawing(seqoid).Statements > among)
            {
                Share(seqoid);
            }

            return;
        }

        // The values left of the sequence's count count no more, for this check and those of other statements.
        Promise(seqoid, 0);
        if (gated || !StatementGated(seqoid))
        {
            WaitWhileShort(seqoid, Math.Max(Math.Max(count, draws), 1));
        }

        // A statement of one row counts no draws for later rows, which it would hold back from the others for nothing.
        if (single)
        {
            s_allowanceEpoch++;
            s_drawingUngated[seqoid] = 0;
            _ = s_sharesTaken.Remove(seqoid);

            // The position the check has just read serves, where it read one.
            WakeAhead(seqoid, s_shortAt.Seqoid == seqoid && (gated || !StatementGated(seqoid)) ? s_shortAt.Position : Position(seqoid), Math.Max(count, 1));
            return;
        }

        Share(seqoid);
        long? position = Position(seqoid);
        if (position is long at)
        {
            s_unnamedMarks[seqoid] = at;
        }

        WakeAhead(seqoid, position, s_drawingUngated[seqoid] + Math.Max(count, 1));
    }

    // Wakes the range worker where the draws a statement is about to take without its sequence's gate bring the
    // sequence past half its active block, or into its last twentieth, as the guard's trigger does once a row's
    // value is there, so that the worker switches before the next statement needs it to.
    private static void WakeAhead(uint seqoid, long? position, long draws)
    {
        if (position is not long from || !s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence) ||
            sequence.Blocks.FindIndex(block => block.State == "active") is not (int index and >= 0))
        {
            return;
        }

        (long lo, long hi, _) = sequence.Blocks[index];
        if (from < lo || from > hi || s_reservedActive.Contains((seqoid, lo)))
        {
            return;
        }

        decimal reach = draws * Step(seqoid);
        long ahead = (long)(sequence.Ascending ? Math.Min(from + reach, hi) : Math.Max(from - reach, lo));
        _ = WakeNearEnd(seqoid, sequence.Ascending, lo, hi, ahead);
    }

    // Waits, a second at most, while a sequence stands outside every block of its this backend knows, as a switch's move
    // to its new block does until the switch commits, reading the blocks again as it goes.
    private static void AwaitMovedBounds(uint seqoid)
    {
        long until = Worker.GetCurrentTimestamp() + 1_000_000;
        while (ReadPosition(seqoid) is long position && s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence) &&
            !sequence.Blocks.Exists(block => block.State == "active" && position >= block.Lo && position <= block.Hi) && Worker.GetCurrentTimestamp() < until)
        {
            PgInterrupts.Check();
            Worker.WaitLatch(1);
            Refresh();
            ForgetBounds();
            s_version = -1;
            Reload();
        }
    }

    // Notes where each sequence the transaction draws from without its gate, and a statement's calls don't name, is as
    // the statement starts, so that its rows count one down only as far as they're seen to draw from it, and waits for
    // the switch of one whose block has fewer values left than a row's calls take, as a row's draws from it would fail.
    private static void StartUnnamed(IReadOnlyCollection<uint> named, int calls)
    {
        if (s_drawingUngated.Count == 0 || s_countingDown || s_namingGrantors || Worf.InOwnSql)
        {
            return;
        }

        s_countingDown = true;
        try
        {
            foreach (uint seqoid in s_drawingUngated.Keys.Where(seqoid => !named.Contains(seqoid)).ToList())
            {
                if (RunOut(seqoid, Position(seqoid), calls) && !StatementGated(seqoid))
                {
                    Promise(seqoid, 0);
                    WaitWhileShort(seqoid, Math.Max(calls, 1));
                    Share(seqoid);
                }

                if (Position(seqoid) is long position)
                {
                    s_unnamedMarks[seqoid] = position;
                }
            }
        }
        finally
        {
            s_countingDown = false;
        }
    }

    // How many values of a sequence were drawn, by any session, since it was last counted down for a call that names no
    // sequence, or since the statement making that call started.
    private static long SeenDrawn(uint seqoid, out long? position)
    {
        position = Position(seqoid);
        long seen = 0;
        if (position is long now && s_unnamedMarks.TryGetValue(seqoid, out long mark) && mark != now)
        {
            decimal step = Step(seqoid);
            seen = (long)Math.Min(Math.Abs((decimal)now - mark) / step, int.MaxValue);
        }

        if (position is long at)
        {
            s_unnamedMarks[seqoid] = at;
        }

        return seen;
    }

    // Whether a sequence has no active block, or has, past the position given, fewer of its active block's values left
    // than a row's calls take and the other statements drawing from it may take, one each before they check again, so
    // that a row's draws from it would fail.
    private static bool RunOut(uint seqoid, long? position, long calls = 1)
    {
        Reload();
        if (!s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence) ||
            sequence.Blocks.FindIndex(block => block.State == "active") is not (int index and >= 0))
        {
            return s_sequences.ContainsKey(seqoid);
        }

        (long lo, long hi, _) = sequence.Blocks[index];
        if (position is not long at)
        {
            return false;
        }

        decimal step = Step(seqoid);
        decimal left = sequence.Ascending ? (decimal)hi - at : (decimal)at - lo;
        return (left / step) - PromisedElsewhere(seqoid) < Math.Max(calls, 1) + Drawing(seqoid).Statements;
    }

    // How many draws a sequence may take before it is checked again: half its steps left before the active block's last
    // twentieth, past those other statements may take before they check again, shared among the statements drawing from
    // it, or none once there.
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
        decimal step = Step(seqoid);
        left -= PromisedElsewhere(seqoid) * step;
        return left <= 0 ? 0 : (long)Math.Min(left / step / 2 / Math.Max(Drawing(seqoid).Statements, 1), int.MaxValue);
    }

    /// <summary>
    /// Notes an executor run that starts: a unit of an enclosing statement that holds gates lets them go meanwhile, as it
    /// draws nothing while the run goes on, which may be long.
    /// </summary>
    /// <param name="queryDesc">The statement run.</param>
    /// <param name="toClient">Whether the run's rows go to the client as they're produced, which may read them slowly,
    /// so that each row lets its gates go as it's produced.</param>
    /// <returns>Whether the run is noted, as worf's own statements, which a unit's check runs, aren't.</returns>
    internal static bool EnterRun(nint queryDesc, bool toClient)
    {
        if (Worf.InOwnSql || s_countingDown || s_namingGrantors)
        {
            return false;
        }

        EnterScope(RunScope, queryDesc, toClient ? 0 : 1);
        return true;
    }

    /// <summary>
    /// Notes an executor run that ends, as a FETCH's does with its cursor still open: the gates its rows left held go, the
    /// draws its sequences could still take without a check go back to other backends' statements, so that its next run
    /// checks first, and the unit of the enclosing statement takes its gates again.
    /// </summary>
    /// <param name="queryDesc">The statement run.</param>
    /// <param name="toClient">Whether the run's rows go to the client, as <see cref="EnterRun"/> was told.</param>
    /// <param name="noted">Whether <see cref="EnterRun"/> noted the run.</param>
    /// <param name="failed">Whether the run failed, whose transaction or subtransaction lets its gates go.</param>
    /// <param name="ended">Whether the statement ran to its end and draws no more.</param>
    internal static void LeaveRun(nint queryDesc, bool toClient, bool noted, bool failed, bool ended)
    {
        if (!noted)
        {
            return;
        }

        LeaveScope(RunScope, queryDesc, toClient ? 0 : 1, failed);
        if (failed)
        {
            return;
        }

        if (ended && s_ungatedDrawers.Count > 0)
        {
            EndDrawing(queryDesc);
        }

        if (s_shareRuns.Count == 0)
        {
            return;
        }

        foreach (uint seqoid in s_shareRuns.Where(share => share.Value == queryDesc).Select(share => share.Key).ToList())
        {
            _ = s_shareRuns.Remove(seqoid);
            if (s_drawingUngated.ContainsKey(seqoid))
            {
                s_allowanceEpoch++;
                s_drawingUngated[seqoid] = 0;
                _ = s_sharesTaken.Remove(seqoid);
            }

            Promise(seqoid, 0);
        }
    }

    /// <summary>
    /// Gives up, as a statement outside any other ends, the draws this backend's statements could still take without a
    /// check, as those a PL/pgSQL block counted outside a plan.
    /// </summary>
    internal static void EndCommand()
    {
        if (s_scopes.Count > 0 || Worf.InOwnSql)
        {
            return;
        }

        s_drewWithin.Clear();
        s_drawAgainWithin.Clear();

        // A block's functions, which were counted among their sequences' drawers outside any statement, draw no more.
        if (s_ungatedDrawers.Count > 0)
        {
            EndDrawing(0);
        }

        foreach (uint seqoid in s_promised.Keys.ToList())
        {
            if (s_drawingUngated.ContainsKey(seqoid))
            {
                s_allowanceEpoch++;
                s_drawingUngated[seqoid] = 0;
                _ = s_sharesTaken.Remove(seqoid);
            }

            Promise(seqoid, 0);
        }

        s_shareRuns.Clear();
    }

    // Starts a scope. A run or a PL/pgSQL statement that starts within a unit suspends it: the gates the enclosing scopes
    // hold go until it ends, as their units draw nothing meanwhile. A row lets go of the gates a call took that started
    // in its run before any row's check, as the first of a statement's calls that start as it runs does.
    private static void EnterScope(byte kind, nint key, nint owner = 0)
    {
        if (kind != RowScope && s_unitGates.Count > 0)
        {
            foreach (uint seqoid in s_unitGates.ToList())
            {
                LetGate(seqoid);
            }
        }
        else if (kind == RowScope && s_scopes.Count > 0 && s_scopes[^1].Kind == RunScope && s_scopeGates.Count > s_scopes[^1].First)
        {
            for (int i = s_scopeGates.Count - 1; i >= s_scopes[^1].First; i--)
            {
                if (s_unitGates.Contains(s_scopeGates[i]))
                {
                    LetGate(s_scopeGates[i]);
                }
            }

            s_scopeGates.RemoveRange(s_scopes[^1].First, s_scopeGates.Count - s_scopes[^1].First);
        }

        uint subxact = 0;
        if (kind >= StatementScope)
        {
            RegisterGates();
            unsafe
            {
                subxact = NativeMethods.GetCurrentSubTransactionId();
            }

            s_statementScopes++;
        }

        s_scopes.Add((kind, key, owner, subxact, s_scopeGates.Count));
        WatchStatements();
    }

    // Ends the innermost scope of a kind that is of a run, a row's node or a PL/pgSQL statement, as LeaveScopeAt does.
    private static void LeaveScope(byte kind, nint key, nint owner = 0, bool failed = false)
    {
        int index = s_scopes.Count - 1;
        while (index >= 0 && ((kind == StatementScope ? s_scopes[index].Kind < StatementScope : s_scopes[index].Kind != kind) ||
            s_scopes[index].Key != key || s_scopes[index].Owner != owner))
        {
            index--;
        }

        if (index >= 0)
        {
            LeaveScopeAt(index, failed);
        }
    }

    // Ends a scope, and those left open within it: the gates they took go, and the scope now innermost, which a run or a
    // PL/pgSQL statement suspended, takes its own again, and hears of the switches committed meanwhile. A scope that
    // failed leaves its gates to the transaction or subtransaction that ends with it.
    private static void LeaveScopeAt(int index, bool failed)
    {
        byte kind = s_scopes[index].Kind;
        CloseScopes(index, !failed);
        if (failed || kind == RowScope || s_scopes.Count == 0)
        {
            return;
        }

        // The gates of the rows and of the run or statement they run in, which were held as the scope started.
        int from = s_scopes.Count - 1;
        while (from > 0 && s_scopes[from].Kind == RowScope)
        {
            from--;
        }

        bool taken = false;
        for (int i = s_scopes[from].First; i < s_scopeGates.Count; i++)
        {
            if (!s_gates.ContainsKey(s_scopeGates[i]))
            {
                TakeGate(s_scopeGates[i], unit: true);
                taken = true;
            }
        }

        if (taken)
        {
            HearOfSwitches();
        }
    }

    // Forgets the scopes from one on, letting the gates they took go unless their transaction or subtransaction does.
    private static void CloseScopes(int index, bool letGo)
    {
        int first = s_scopes[index].First;
        for (int i = s_scopeGates.Count - 1; letGo && i >= first; i--)
        {
            if (s_unitGates.Contains(s_scopeGates[i]))
            {
                LetGate(s_scopeGates[i]);
            }
        }

        s_scopeGates.RemoveRange(first, s_scopeGates.Count - first);
        for (int i = index; i < s_scopes.Count; i++)
        {
            s_statementScopes -= s_scopes[i].Kind >= StatementScope ? 1 : 0;
        }

        s_scopes.RemoveRange(index, s_scopes.Count - index);
        WatchStatements();
    }

    // Takes a sequence's gate for the unit innermost, a row or a PL/pgSQL statement, or the run a call starts in outside
    // a row, unless the backend holds it, so that a switch beside the unit moves the sequence only once the unit has
    // drawn, and the unit reads the bounds of any switch before it. A call outside any, as one a plan starts before it
    // runs, takes none, as it would hold it while the session idles.
    private static void GateUnit(uint seqoid)
    {
        if (s_scopes.Count == 0)
        {
            return;
        }

        if (s_gates.ContainsKey(seqoid))
        {
            return;
        }

        TakeGate(seqoid, unit: true);
        s_scopeGates.Add(seqoid);
    }

    // Whether the unit now running, a row or the run or PL/pgSQL statement it runs in, took a sequence's gate, which a
    // scope within it may have suspended since, or a statement made its own.
    private static bool UnitTook(uint seqoid)
    {
        if (s_scopes.Count == 0)
        {
            return false;
        }

        int from = s_scopes.Count - 1;
        while (from > 0 && s_scopes[from].Kind == RowScope)
        {
            from--;
        }

        return s_scopeGates.IndexOf(seqoid, s_scopes[from].First) >= 0;
    }

    // Reads the blocks and bounds again once a switch has committed since this backend last read them, as one it waited
    // for at a gate has.
    private static void HearOfSwitches()
    {
        if (SequenceRanges.Version.Value != s_version || SequenceRanges.LocalVersion != s_localVersion)
        {
            Refresh();
            ForgetBounds();
            Reload();
        }
    }

    // Whether a statement that writes, as an insert or a COPY, holds a sequence's gate, and checks it at its rows itself.
    private static bool StatementGated(uint seqoid) => s_gating.Exists(entry => entry.Sequences.Contains(seqoid));

    // Gives a statement drawing from a sequence without its gate its count of draws before it checks the sequence again,
    // and counts them in the shared table, which other statements' checks leave them.
    private static void Share(uint seqoid)
    {
        // A statement's first count is a thirty-second of what it could count, and each count after it, as the one
        // before runs out, twice as much, up to all of it. A statement that starts alone would else count half of
        // what the block has left at once, before the statements that start beside it are there to share it with,
        // and hold it back from them for as long as it runs, or stalls: they would find the block short with most
        // of it still left, and wait for a switch off it.
        int taken = s_sharesTaken.GetValueOrDefault(seqoid);
        s_sharesTaken[seqoid] = Math.Min(taken + 1, FullShareAfter);
        long Counted(long could) => Math.Min(could, Math.Max(could >> (FullShareAfter - taken), PromiseBatch));
        long rows = Counted(RowsUntilShort(seqoid));
        Promise(seqoid, rows);

        // The count holds only once the other statements can see it, so the values left are read again: those drawn
        // between the first read and then, as while this backend waited for a processor, are none of the count's.
        if (rows > 0 && Counted(RowsUntilShort(seqoid)) is long left && left < rows)
        {
            rows = left;
            Promise(seqoid, rows);
        }

        s_allowanceEpoch++;
        s_drawingUngated[seqoid] = rows;
        s_sharedAmong[seqoid] = Drawing(seqoid).Statements;
        int run = s_scopes.Count - 1;
        while (run >= 0 && s_scopes[run].Kind != RunScope)
        {
            run--;
        }

        // The count of a statement that draws again within a command is the command's: it goes back as the statement
        // or block around the statement ends, so the next turn of a loop finds it. A statement with nothing around
        // it keeps its count its own.
        if (run >= 0 && DrawsAgainWithin(seqoid))
        {
            int around = run - 1;
            while (around >= 0 && s_scopes[around].Kind != RunScope)
            {
                around--;
            }

            if (around >= 0 || s_commandDepth > 0)
            {
                run = around;
            }
        }

        s_shareRuns[seqoid] = run < 0 ? 0 : s_scopes[run].Key;
    }

    // How many utility statements a client sent are running: a statement that starts under one, as each of a DO
    // block's, runs within its command.
    private static int s_commandDepth;

    // The sequences the statements of the command under way have drawn from without their gates, and those a second
    // such statement drew from, whose draws are the command's from then on.
    private static readonly HashSet<uint> s_drewWithin = [];
    private static readonly HashSet<uint> s_drawAgainWithin = [];

    /// <summary>
    /// Notes a utility statement a client sent, as it starts and ends.
    /// </summary>
    /// <param name="change">One as it starts, minus one as it ends.</param>
    internal static void CountCommand(int change)
    {
        // A command that failed left what its statements drew from noted.
        if (change > 0 && s_commandDepth == 0)
        {
            s_drewWithin.Clear();
            s_drawAgainWithin.Clear();
        }

        s_commandDepth = Math.Max(s_commandDepth + change, 0);
    }

    // Whether a statement that starts does so within another statement's run, or within a utility statement, as a
    // function's or a DO block's statements do.
    private static bool Within() => s_commandDepth > 0 || s_scopes.Exists(scope => scope.Kind == RunScope);

    // Notes the sequences a statement that starts draws from without their gates: a statement that starts on its
    // own begins a command, and one within a command that draws from a sequence an earlier one of it drew from
    // makes the sequence's draws the command's.
    private static void NoteDrawsWithin(List<uint> sequences)
    {
        if (!Within())
        {
            s_drewWithin.Clear();
            s_drawAgainWithin.Clear();
            return;
        }

        s_drawAgainWithin.UnionWith(sequences.Where(s_drewWithin.Contains));
        s_drewWithin.UnionWith(sequences);
    }

    // Whether a sequence's draws are those of the command under way, as the statements within it draw from it one
    // after another.
    private static bool DrawsAgainWithin(uint seqoid) => s_drawAgainWithin.Count > 0 && s_drawAgainWithin.Contains(seqoid);

    // Counts the draws this backend may still take from a sequence without checking it in the shared table, in place of
    // those it counted.
    private static void Promise(uint seqoid, long rows)
    {
        long change = rows - s_promised.GetValueOrDefault(seqoid).Promised;
        if (change != 0)
        {
            CountDrawer(seqoid, 0, promised: change);
            _ = PgAtomic.Add(SequenceRanges.PromisedDraws, change);
        }

        if (rows > 0)
        {
            s_allowanceEpoch++;
            s_promised[seqoid] = (rows, 0);
        }
        else
        {
            s_allowanceEpoch++;
            _ = s_promised.Remove(seqoid);
        }

        HookExit();
    }

    // Notes draws taken of the count, which the shared table counts no more once there are a few of them, an eighth of
    // the count at least, so that other statements have the values the count no longer holds back without this backend
    // taking the table's lock at every row, and tells whether it told the shared table of them.
    private static bool Drew(uint seqoid, long draws)
    {
        bool told = false;
        if (s_promised.TryGetValue(seqoid, out (long Promised, long Drawn) promised))
        {
            promised.Drawn += draws;
            if (promised.Drawn >= Math.Max(PromiseBatch, promised.Promised / 8))
            {
                long taken = Math.Min(promised.Drawn, promised.Promised);
                CountDrawer(seqoid, 0, promised: -taken);
                _ = PgAtomic.Add(SequenceRanges.PromisedDraws, -taken);
                promised = (promised.Promised - taken, 0);
                told = true;
            }

            s_allowanceEpoch++;
            s_promised[seqoid] = promised;
        }

        return told;
    }

    // The sequence WaitWhileShort last checked, with the position it read of it last, if it read one.
    private static (uint Seqoid, long? Position) s_shortAt;

    // How many statements drew from each sequence as this backend last took its count of it.
    private static readonly Dictionary<uint, int> s_sharedAmong = [];

    // How many counts of each sequence this backend's statement has taken so far, up to the number after which a
    // count is all the statement could count.
    private static readonly Dictionary<uint, int> s_sharesTaken = [];
    private const int FullShareAfter = 5;

    private const long PromiseBatch = 16;

    // The draws other backends' statements may take from a sequence before they check it again, read from the shared
    // table only while some statement anywhere may.
    private static long PromisedElsewhere(uint seqoid)
        => SequenceRanges.PromisedDraws.Value == 0 ? 0 : Math.Max(Drawer(seqoid).Promised - s_promised.GetValueOrDefault(seqoid).Promised, 0);

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
    {
        Starting(() => Draws(CopyDefaultsText(relid, columns, defaults) + " " + condition + " " + WrittenChecks([relid])), copy: true);

        // An identity column's default starts no call of nextval, so what the start of one notes of a COPY's is
        // noted here, for each sequence such a column of the COPY draws from without its gate: the COPY waits for
        // its grantor and for a block with values left, and counts among those drawing from it.
        List<uint>? identities = s_copyIdentities;
        s_copyIdentities = null;
        WorfStartingStatement starting = s_starting[^1];
        foreach (uint seqoid in identities ?? [])
        {
            if (StatementGated(seqoid) || !starting.Switched.Add(seqoid))
            {
                continue;
            }

            if (s_grantors.ContainsKey(seqoid) && !s_confirmedSequences.Contains(seqoid))
            {
                WaitForGrantor(seqoid);
            }

            List<uint> drawn = [seqoid];
            CountUngated(drawn);
            WaitWhileShort(seqoid);
            DrawUngated(drawn);
        }
    }

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
        Starting(() => RunDraws(statement, started, queryDesc->planstate), running: (nint)queryDesc);
        return true;
    }

    /// <summary>
    /// Forgets the statement noted last, once its expressions have started.
    /// </summary>
    internal static void StartedExpressions()
    {
        WorfStartingStatement started = s_starting[^1];
        s_starting.RemoveAt(s_starting.Count - 1);

        // A COPY's calls that went through the guard leave their places to the statements after it.
        if (started.CopyState != 0)
        {
            FreePlanSites(started.CopyState);
        }
    }

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
    private static unsafe void Starting(Func<(HashSet<uint> Called, int Calls, bool Unnamed)> draws, bool explained = false, nint running = 0, bool copy = false)
        => s_starting.Add(new(draws, (nint)NativeGlobals.error_context_stack, explained, running: running, copy: copy));

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
    private static unsafe (HashSet<uint> Called, int Calls, bool Unnamed) RunDraws(PlannedStmt* statement, int started, PlanState* state)
    {
        // The rows of a long VALUES list that hold only constants and calls that name their sequence are read where
        // they are, as their text, which says no more, takes longer to write and read than the statement to run.
        HashSet<uint> plain = [];
        HashSet<uint> plainTypes = [];
        int plainCalls = 0;
        List<(nint Plan, nint Rows, nint Others)> lists = [];
        string text;
        try
        {
            SetPlainRowsAside(state, plain, plainTypes, ref plainCalls, lists);
            text = PlanText((Node*)statement);
        }
        finally
        {
            foreach ((nint plan, nint rows, _) in lists)
            {
                ((ValuesScan*)plan)->values_lists = (List*)rows;
            }
        }

        foreach ((_, _, nint others) in lists)
        {
            NativeMethods.list_free((List*)others);
        }

        (HashSet<uint> called, int calls, bool unnamed) = Draws(text);
        called.UnionWith(plain);
        calls = Math.Max(calls + plainCalls - started, 0);
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

        long[] types = [.. NamedTypes(text).Union(plainTypes).Select(type => (long)type)];
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

    // Takes the rows that hold only constants, parameters and calls of nextval whose sequence a constant names out of
    // a plan's VALUES lists of 64 rows or more, until the caller puts the lists back, and reads those rows where they
    // are: the sequences their calls name, how many calls they make, and their values' types.
    private static unsafe void SetPlainRowsAside(PlanState* state, HashSet<uint> called, HashSet<uint> types, ref int calls,
        List<(nint Plan, nint Rows, nint Others)> lists)
    {
        List<(nint Memory, nint Node)> nodes = [];
        ValuesRows(state, nodes, []);
        foreach ((_, nint node) in nodes)
        {
            ValuesScan* plan = (ValuesScan*)((PlanState*)node)->plan;
            List* rows = plan is null ? null : plan->values_lists;
            if (rows is null || rows->length < 64 || lists.Exists(list => list.Plan == (nint)plan))
            {
                continue;
            }

            List* others = null;
            for (int i = 0; i < rows->length; i++)
            {
                List* row = (List*)rows->elements[i].ptr_value;
                if (PlainRow(row, null, null) is int made)
                {
                    calls += made;
                    _ = PlainRow(row, called, types);
                }
                else
                {
                    others = NativeMethods.lappend(others, row);
                }
            }

            lists.Add(((nint)plan, (nint)rows, (nint)others));
            plan->values_lists = others;
        }
    }

    // Reads a VALUES list's row that holds only constants, parameters and calls of nextval whose sequence a constant
    // names: how many calls it makes, with the sequences they name and its values' types for a caller that wants
    // them, or null for a row that holds anything else.
    private static unsafe int? PlainRow(List* row, HashSet<uint>? called, HashSet<uint>? types)
    {
        if (row is null || row->type != NodeTag.T_List)
        {
            return null;
        }

        int calls = 0;
        for (int i = 0; i < row->length; i++)
        {
            Node* value = (Node*)row->elements[i].ptr_value;
            while (value is not null && value->type == NodeTag.T_RelabelType)
            {
                _ = types?.Add(((RelabelType*)value)->resulttype);
                value = (Node*)((RelabelType*)value)->arg;
            }

            if (value is null)
            {
                return null;
            }

            switch (value->type)
            {
                case NodeTag.T_Const:
                    _ = types?.Add(((Const*)value)->consttype);
                    break;
                case NodeTag.T_Param:
                    _ = types?.Add(((Param*)value)->paramtype);
                    break;
                case NodeTag.T_FuncExpr when ((FuncExpr*)value)->funcid == NextvalOid && NamedByCall(value) is uint named and not 0 && NamesRegclass(value):
                    _ = called?.Add(named);
                    calls++;
                    break;
                default:
                    return null;
            }
        }

        return calls;
    }

    // Whether a call's one argument is a constant of type regclass.
    private static unsafe bool NamesRegclass(Node* call)
    {
        List* args = ((FuncExpr*)call)->args;
        return args is not null && args->length == 1 && ((Node*)args->elements[0].ptr_value)->type == NodeTag.T_Const &&
            ((Const*)args->elements[0].ptr_value)->consttype == RegclassOid;
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

        // Ordinary tables' are read from PostgreSQL's cache, which the statement that writes them has loaded.
        string?[] cached = [.. relids.Select(relid => CachedTriggerText((uint)relid, afterInserts: false))];
        if (Array.TrueForAll(cached, text => text is not null))
        {
            return WithDomainChecks(string.Concat(cached));
        }

        // Those of partitioned tables and their partitions are read straight from the catalogs.
        StringBuilder text = new();
        foreach (long relid in relids)
        {
            List<uint> tables = [(uint)relid, .. Under((uint)relid)];
            foreach (uint table in tables)
            {
                _ = text.Append(CheckText(table));
            }

            foreach ((_, _, uint function, _, _, _, _, string? condition) in TreeTriggers((uint)relid))
            {
                _ = text.Append(condition).Append(condition is null ? "" : " ").Append(CultureInfo.InvariantCulture, $"{{TRIGGER :funcid {function} }} ");
            }
        }

        return WithDomainChecks(text.ToString());
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
    /// <summary>
    /// Has the guard hear of the changes to tables and schemas after which worf's own tables are looked up again.
    /// </summary>
    internal static void WatchCatalogs() => WatchTables();

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
        if (relid == 0)
        {
            s_steps.Clear();
        }
        else
        {
            _ = s_steps.Remove(relid);
        }

        s_namedByText.Clear();
        s_drawsByText.Clear();
        s_writtenCalls.Clear();
        Worf.ForgetCatalogs();
        s_tablesStamp++;

        // What a table's triggers draw counts its partitions' too, which a change to a partition changes.
        s_drawnBy.Clear();
        s_afterConditionsDraw.Clear();
        s_arguedBy.Clear();
        s_arguedFunctions.Clear();
        s_guarded.Clear();
        s_checked.Clear();
        s_subscriptionDraws.Clear();
        s_triggerArguments.Clear();
        if (relid == 0)
        {
            foreach (WorfGuardConditions conditions in s_conditions.Values)
            {
                conditions.Stale = true;
            }
        }
        else if (s_conditions.TryGetValue(relid, out WorfGuardConditions? conditions))
        {
            conditions.Stale = true;
        }

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
        s_writtenCalls.Clear();
        s_domainDraws = null;
        s_guardFunctions.Clear();
        Worf.ForgetCatalogs();
        s_tablesStamp++;
        foreach (WorfGuardConditions conditions in s_conditions.Values)
        {
            conditions.Stale = true;
        }

        s_plpgsqlCasts.Clear();
        s_checksUnnamed.Clear();
        s_namedByText.Clear();
        s_drawsByText.Clear();
        s_subscriptionDraws.Clear();

        // A native subscription's tables change as REFRESH PUBLICATION adds one, which sends no relcache invalidation.
        s_triggerArguments.Clear();
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
        // The text is kept until the table, a type or a constraint changes: read for each COPY, PostgreSQL would keep
        // the query's plan for the session too.
        WatchTables();
        (uint, string) key = (relid, "copy:" + (defaults ? "d:" : ":") + (columns is null ? "*" : string.Join(',', columns)));
        if (!s_defaultsText.TryGetValue(key, out string? text))
        {
            text = ReadCopyDefaultsText(relid, columns, defaults);
            s_defaultsText[key] = text;
        }

        return text;
    }

    private static string ReadCopyDefaultsText(uint relid, short[]? columns, bool defaults)
    {
        if (CachedDefaultsText(relid, columns, defaults) is string cached)
        {
            return WithDomainChecks(cached);
        }

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
        // worf's own reading of what a statement names starts no call of the statement's, and a statement that goes
        // by what the guard kept of it starts its calls as it did when the guard read them.
        if (s_namingGrantors || s_settledStarting.Query != 0)
        {
            return;
        }

        // A call that starts in a VALUES list's row, as the one before it in the statement did, comes to what that one
        // came to while nothing the guard goes by has changed: it counts its draw down, and reads nothing.
        if (StartsAsSettled())
        {
            return;
        }

        // Each call that starts counts against the calls the expressions starting around it name.
        bool plpgsql;
        nint simple;
        nint inUse;
        nint estate;
        nint valuesRow = 0;
        WorfStartingStatement? starting = null;
        unsafe
        {
            plpgsql = InPlpgsql(out simple, out inUse, out estate);
            if (plpgsql && simple != 0)
            {
                // A start that failed, as one cancelled within its first call's check, left PL/pgSQL's mark of the
                // expression's transaction as it was, so the start is told by this transaction and subtransaction too.
                (uint, uint, uint) start = (((PLpgSQL_expr*)simple)->expr_simple_lxid, Lxid(), NativeMethods.GetCurrentSubTransactionId());
                if (s_simple.Expr != simple || s_simple.Start != start)
                {
                    s_simple = (simple, start, null, 0);
                }

                s_simple.Looked++;
            }
        }

        if (!plpgsql && s_starting.Count > 0)
        {
            starting = s_starting[^1];
            starting.Looked++;
            if (starting.Running != 0)
            {
                unsafe
                {
                    valuesRow = ValuesRow((QueryDesc*)starting.Running);
                    if (valuesRow != 0)
                    {
                        int row = ((ValuesScanState*)valuesRow)->curr_idx;
                        s_valuesRow = s_valuesRow.Node == valuesRow && s_valuesRow.Row == row ? s_valuesRow with { Started = s_valuesRow.Started + 1 } : (valuesRow, row, 1);
                    }
                }

                // A call of a VALUES list's row is checked as it starts and never runs again, so it isn't one to find.
                if (valuesRow == 0)
                {
                    s_runStarted[starting.Running] = s_runStarted.GetValueOrDefault(starting.Running) + 1;
                }
            }
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
        List<uint> counted = [];
        bool unnamed = false;
        bool anyOf = false;
        HashSet<uint> spelled = [];
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

            // A PL/pgSQL expression whose calls don't name their sequence, as one computed as it runs, may draw from
            // any, and its function's statements count down for each; and so may a statement's, as a name a column holds
            // or a parameter a generic plan leaves unknown.
            if (s_sequences.Count > 0 && (plpgsql ? simple != 0 && s_simple.Draws is { Unnamed: true } : starting is { Draws.Unnamed: true }))
            {
                spelled = named ?? [];
                named = [.. s_sequences.Keys];
                anyOf = true;
            }

            if (named is not null)
            {
                // A call that starts as its statement runs, after one of the statement's has, counts down: a VALUES list
                // starts each row's calls as it reaches the row.
                if (starting is { Running: not 0 })
                {
                    counted = [.. named.Where(seqoid => starting.Switched.Contains(seqoid) && s_drawingUngated.ContainsKey(seqoid))];
                }

                switching = [.. named.Where(seqoid => seqoid != snowflake && s_sequences.ContainsKey(seqoid) && !StatementGated(seqoid) &&
                    (plpgsql || starting!.Switched.Add(seqoid)))];
            }

            // A PL/pgSQL expression starts its calls once a transaction, so each goes through the guard from the
            // expression's next run on.
            if (plpgsql && simple != 0 && s_sequences.Count > 0)
            {
                StartSite(simple, estate);
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

        // A statement PostgreSQL compiles has its calls go through the fmgr hook from the start: PostgreSQL asks the
        // hook check about each as it compiles it, once the expression's calls have started, so what the hook needs is
        // read before then.
        if (starting is { Compiles: true, Switched.Count: > 0, Hooked: 0 } && Executor.CountsDraws)
        {
            ReadNextval();
            ReadHookedCall();
            s_snowflake = Worf.CatalogOid("snowflake_node");
            RegisterEnd();
            HookExit();
        }

        // A COPY's tables start their CHECK constraints and trigger conditions as their first rows come, and run them
        // for every row after, so the row after this one looks for the calls and has them go through the guard.
        if (!plpgsql && starting is { Copy: true, CopyWatched: false })
        {
            WatchCopy(starting);
        }

        bool settles = waiting.Count == 0 && switching.Count == 0;
        foreach (uint seqoid in waiting)
        {
            WaitForGrantor(seqoid, unnamed);
        }

        // A call that first starts as its statement runs is its row's, which may make the row's other calls too before the
        // count down the statement's later rows take.
        // One whose sequence they don't name waits so only for a sequence whose block has fewer values left than a row's
        // calls take, as a row's draws from it would fail, and its rows count the others down as they're seen to draw
        // from them.
        long rowDraws = switching.Count > 0 && starting is { Running: not 0 } running ? RunningRowCalls(running.Running, valuesRow) : 1;
        long rowCalls = Math.Max(rowDraws, starting?.Draws.Calls ?? 1);
        if (!plpgsql && starting is not null)
        {
            // A statement that runs within another, or within a block, after one that drew from the same sequence
            // there, as each turn of a loop of inserts does, is counted with the statement or block around it, as a
            // PL/pgSQL expression's draws are: the draws it could still take stay its command's, for the next turn.
            List<uint> again = Within() ? [.. switching.Where(s_drewWithin.Contains)] : [];
            if (again.Count > 0)
            {
                CountFunction(again);
                CountUngated([.. switching.Where(seqoid => !again.Contains(seqoid))]);
            }
            else
            {
                CountUngated(switching);
            }

            NoteDrawsWithin(switching);
        }
        else if (plpgsql && switching.Count > 0)
        {
            CountFunction(switching);
        }

        // A plan that names its sequences leaves their checks to its first row.
        if (!plpgsql && !anyOf && starting is { Planned: true })
        {
            DeferUngated(switching);
            switching = [];
        }

        if (switching.Count > 0)
        {
            GateStarting(plpgsql, estate, starting, switching);
        }

        foreach (uint seqoid in switching.Where(seqoid => !anyOf || spelled.Contains(seqoid) || RunOut(seqoid, Position(seqoid), rowCalls)))
        {
            WaitWhileShort(seqoid, anyOf && !spelled.Contains(seqoid) ? rowCalls : rowDraws);
        }

        if (switching.Count > 0)
        {
            DrawUngated(switching);
        }

        // A statement whose calls first start as it runs, as those of a CHECK constraint or a trigger's WHEN condition
        // do, which then run for each row, counts its rows down from then on, until the calls are found in the states
        // its tables keep, and again from a call that starts later, as a partition's does with its first row.
        // A row of a VALUES list starts its calls anew, so each is checked as it starts and the rows need no count,
        // unless the statement's rows go to the client, which may read them slowly: the plan's top node then counts
        // them, which lets a row's gates go as it's produced.
        if ((switching.Count > 0 || counted.Count > 0) && starting is { Running: not 0 } && RowsAreToCount(starting.Running, valuesRow))
        {
            CountRunningRows(starting.Running, valuesRow);
        }

        List<uint> down = anyOf ? [.. counted.Where(spelled.Contains)] : counted;
        if (counted.Count > 0)
        {
            CountDownDraws(valuesRow != 0 ? s_valuesRow.Started : 1, down, unnamed: anyOf);
        }

        // A VALUES row's call that waited for nothing and switched nothing leaves the row's next calls what it came to.
        if (valuesRow != 0 && starting is not null && settles)
        {
            Settle(starting, counted.Count > 0 ? down : [], anyOf);
        }
    }

    // Notes what a VALUES row's call came to, with what the guard had read then, for the row's next calls.
    private static unsafe void Settle(WorfStartingStatement starting, List<uint> counted, bool unnamed)
    {
        if (s_fastDraws is not null)
        {
            starting.Settled = ([.. counted], unnamed, Volatile.Read(ref s_fastDraws[WorfFastDraws.Changes]), SequenceRanges.LocalVersion,
                s_drawingUngated.Count, starting.Switched.Count);
        }
    }

    // Has a COPY's next row look for the calls of nextval its tables started with this one: PostgreSQL empties the
    // memory of the executor state's rows before each row, and tells whoever asked as it does, once. The state is the
    // one PostgreSQL builds the starting expression in, its own memory's first object. A call that starts elsewhere, as
    // a row is read, is that of a domain a column holds, which a column that held nothing until now starts late: the
    // state is then the one the COPY's first row found.
    private static unsafe void WatchCopy(WorfStartingStatement copy)
    {
        EState* state = ExecutorStateOf(NativeGlobals.CurrentMemoryContext);
        if (state is null && copy.CopyRead != 0)
        {
            state = (EState*)copy.CopyState;
        }

        if (state is null || state->es_per_tuple_exprcontext is null || (copy.CopyState != 0 && copy.CopyState != (nint)state))
        {
            return;
        }

        ReadNextval();
        ReadHookedCall();
        RegisterEnd();
        HookExit();
        s_snowflake = Worf.CatalogOid("snowflake_node");
        MemoryContextCallback* told = (MemoryContextCallback*)NativeMethods.MemoryContextAllocZero(state->es_query_cxt, (nuint)sizeof(MemoryContextCallback));
        told->func = new MemoryContextCallbackFunction((delegate* unmanaged[Cdecl]<void*, void>)&CopyRowDone);
        told->arg = state;
        NativeMethods.MemoryContextRegisterResetCallback(state->es_per_tuple_exprcontext->ecxt_per_tuple_memory, told);
        copy.CopyState = (nint)state;
        copy.CopyWatched = true;
    }

    // The executor state whose memory a context is, or null: PostgreSQL makes the state the first object in a context
    // it names ExecutorState, so it lies a little way in, tells its kind and names the context as its own.
    private static unsafe EState* ExecutorStateOf(MemoryContextData* memory)
    {
        if (memory is null || memory->name is null || !MemoryMarshal.CreateReadOnlySpanFromNullTerminated((byte*)memory->name).SequenceEqual("ExecutorState"u8))
        {
            return null;
        }

        for (int offset = 64; offset <= 512; offset += 8)
        {
            EState* state = (EState*)((byte*)memory + offset);
            if (state->type == NodeTag.T_EState && state->es_query_cxt == memory)
            {
                return state;
            }
        }

        return null;
    }

    // Runs as PostgreSQL empties the memory of a COPY's rows, before the row after one whose table started a call of
    // nextval, or as the COPY ends: the calls its tables keep go through the guard from here on. PostgreSQL calls
    // this straight, so it may call nothing of PostgreSQL's and let no exception out.
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void CopyRowDone(void* state)
    {
        try
        {
            for (int i = s_starting.Count - 1; i >= 0; i--)
            {
                WorfStartingStatement copy = s_starting[i];
                if (!copy.Copy || copy.CopyState != (nint)state)
                {
                    continue;
                }

                copy.CopyWatched = false;
                GuardCopyCalls((EState*)state, copy);
                return;
            }
        }
        catch (Exception failure) when (failure is NullReferenceException or InvalidOperationException or ArgumentException or
            IndexOutOfRangeException or KeyNotFoundException)
        {
            // Nothing above throws unless worf's own state is wrong. The next statement that starts reports it.
            Executor.NoteHookFailure(failure);
        }
    }

    // Has the calls of nextval that a COPY's tables keep, as they started with its rows, go through the guard, and
    // those of its condition and of the domains its columns hold, once its first row found them.
    private static unsafe void GuardCopyCalls(EState* state, WorfStartingStatement copy)
    {
        List<(nint Step, bool Single, bool Compiled)> calls;
        s_findingAlone = true;
        try
        {
            calls = RunCalls(state);
            if (copy.CopyRead != 0)
            {
                FindReadCalls((CopyFromStateData*)copy.CopyRead, calls, []);
            }
        }
        finally
        {
            s_findingAlone = false;
        }

        if (!s_planSites.TryGetValue((nint)state, out List<(int Site, nint Call, nint Information)>? sites))
        {
            sites = [];
        }

        foreach ((nint call, _, bool compiled) in calls)
        {
            ExprEvalStep* step = (ExprEvalStep*)call;
            _ = copy.CopyCalls.Add(call);
            int site = compiled ? -1 : GuardCall(step);
            if (site >= 0)
            {
                sites.Add((site, (nint)step->d.func.finfo->fn_expr, (nint)step->d.func.fcinfo_data));
            }
        }

        if (sites.Count > 0)
        {
            s_planSites[(nint)state] = sites;
        }
    }

    // Counts down for a call that starts in a VALUES list's row of a running statement whose last such call settled
    // what they come to, while the blocks, the sequences drawn from and those the statement switched are what they
    // were then. Tells whether it did.
    private static unsafe bool StartsAsSettled()
    {
        if (s_starting.Count == 0 || s_starting[^1] is not { Running: not 0, Settled: { } settled } starting || s_fastDraws is null ||
            settled.Changes != Volatile.Read(ref s_fastDraws[WorfFastDraws.Changes]) || settled.Changes != s_changesRead || s_version < 0 ||
            settled.Local != SequenceRanges.LocalVersion || settled.Local != s_localVersion || settled.Ungated != s_drawingUngated.Count ||
            settled.Switched != starting.Switched.Count)
        {
            return false;
        }

        nint valuesRow = ValuesRow((QueryDesc*)starting.Running);
        if (valuesRow == 0)
        {
            return false;
        }

        int row = ((ValuesScanState*)valuesRow)->curr_idx;
        s_valuesRow = s_valuesRow.Node == valuesRow && s_valuesRow.Row == row ? s_valuesRow with { Started = s_valuesRow.Started + 1 } : (valuesRow, row, 1);
        starting.Looked++;
        if (settled.Counted.Length > 0)
        {
            CountDownDraws(s_valuesRow.Started, settled.Counted, unnamed: settled.Unnamed);
        }

        return true;
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

    // The search path, as the session has it now: the setting's own text, read where PostgreSQL keeps it, which is
    // made into a string only when it has changed.
    private static unsafe string SearchPath()
    {
        ReadOnlySpan<byte> path = MemoryMarshal.CreateReadOnlySpanFromNullTerminated((byte*)NativeGlobals.namespace_search_path);
        if (s_searchPath is null || !path.SequenceEqual(s_searchPathBytes))
        {
            s_searchPathBytes = path.ToArray();
            s_searchPath = Worf.FromServer(NativeGlobals.namespace_search_path);
        }

        return s_searchPath;
    }

    private static string? s_searchPath;
    private static byte[] s_searchPathBytes = [];

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
    /// Tells whether <see cref="StartingTrigger"/> has anything to check for a function that starts, from what the
    /// guard has noted alone, as the fmgr hook asks where it may call nothing of PostgreSQL's.
    /// </summary>
    /// <param name="fnOid">The function.</param>
    /// <returns>Whether it is a trigger function the guard watches.</returns>
    internal static bool WatchesTrigger(uint fnOid)
    {
        if (s_hookedTriggers.Contains(fnOid) || IsGuardFunction(fnOid))
        {
            return true;
        }

        foreach ((nint, uint, Dictionary<uint, HashSet<uint>> Functions) hooking in s_hookingStatements)
        {
            if (hooking.Functions.ContainsKey(fnOid))
            {
                return true;
            }
        }

        return false;
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
        if (s_hookingStatements.Exists(entry => entry.Functions.ContainsKey(fnOid)))
        {
            return true;
        }

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
        List<uint> argued = [];
        s_namingGrantors = true;
        try
        {
            Reload();
            if (s_sequences.Count == 0 || !Triggers(relid).Argued)
            {
                return;
            }

            uint snowflake = Worf.CatalogOid("snowflake_node");
            argued = [.. (update ? ArguedBy(relid, TriggerTypeUpdate, TriggerTypeInsert) : ArguedBy(relid, TriggerTypeInsert)).Where(seqoid => seqoid != snowflake)];
            waiting = [.. argued.Where(seqoid => s_grantors.ContainsKey(seqoid) && !s_confirmedSequences.Contains(seqoid)).Order()];
        }
        finally
        {
            s_namingGrantors = false;
        }

        foreach (uint seqoid in waiting)
        {
            WaitForGrantor(seqoid);
        }

        // The row the trigger's C code draws for counts down, so that rows applied one after another don't run the
        // block out.
        CountDownArgued(argued);
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

        int hooking = s_hookingStatements.FindLastIndex(entry => entry.Functions.ContainsKey(fnOid));
        if ((hooking < 0 && !s_hookedTriggers.Contains(fnOid)) || quiet || s_namingGrantors || SequenceRanges.Recovering() ||
            Worf.CatalogOid("sequence_range_block") == 0)
        {
            return;
        }

        // A statement's trigger function checks the sequences its triggers name, for the grantor and as a row of the
        // statement draws from them.
        if (hooking >= 0)
        {
            uint snowflakeSequence = Worf.CatalogOid("snowflake_node");
            List<uint> sequences = [.. s_hookingStatements[hooking].Functions[fnOid].Where(seqoid => seqoid != snowflakeSequence)];
            List<uint> unanswered;
            s_namingGrantors = true;
            try
            {
                Reload();
                unanswered = [.. sequences.Where(seqoid => s_grantors.ContainsKey(seqoid) && !s_confirmedSequences.Contains(seqoid)).Order()];
            }
            finally
            {
                s_namingGrantors = false;
            }

            foreach (uint seqoid in unanswered)
            {
                WaitForGrantor(seqoid);
            }

            CountDownArgued(sequences);
            return;
        }

        List<uint> waiting = [];
        List<uint> argued = [];
        List<uint>? named;
        s_namingGrantors = true;
        try
        {
            Reload();
            if (Worf.InOwnSql || s_sequences.Count == 0)
            {
                return;
            }

            WatchTables();
            uint subscription;
            unsafe
            {
                subscription = NativeGlobals.MySubscription is null ? 0 : NativeGlobals.MySubscription->oid;
            }

            if (!s_triggerArguments.TryGetValue((fnOid, subscription), out named))
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
            argued = [.. named.Where(seqoid => seqoid != snowflake && s_sequences.ContainsKey(seqoid))];
        }
        finally
        {
            s_namingGrantors = false;
        }

        foreach (uint seqoid in waiting)
        {
            WaitForGrantor(seqoid);
        }

        CountDownArgued(argued);
    }

    // Counts a row a trigger's C code draws for down, for the sequences it names: one it hasn't drawn from this
    // transaction checks for the switch first, and the rest count down, with their gates held too, as the call starts
    // between the statement's rows.
    private static void CountDownArgued(List<uint> sequences)
    {
        if (sequences.Count == 0)
        {
            return;
        }

        List<uint> first = [.. sequences.Where(seqoid => !s_drawingUngated.ContainsKey(seqoid))];
        foreach (uint seqoid in first)
        {
            WaitWhileShort(seqoid);
        }

        if (first.Count > 0)
        {
            DrawUngated(first);
        }

        CountDownDraws(1, [.. sequences.Except(first)], gated: true);
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
                // PL/pgSQL marks an expression in use as it runs it and clears the mark as the run ends, so a run that
                // failed leaves it set, and PL/pgSQL goes by it only in the transaction it started the expression in.
                if (expr->expr_simple_in_use && expr->expr_simple_lxid == Lxid())
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

        // PostgreSQL's cache of functions answers, which the statement that calls them has filled.
        uint own;
        unsafe
        {
            own = NativeMethods.get_namespace_oid(Worf.PStrDup(Worf.ExtensionName), true);
        }

        bool any = false;
        foreach (long function in unread)
        {
            bool checks = false;
            if (FunctionRow((uint)function) is (uint language, uint schema, _, _, string source))
            {
                checks = language == InternalLanguage ? s_inputSources.Contains(source)
                    : language is not (CLanguage or SqlLanguage) && schema != own && LanguageName(language) != "plpgsql";
            }

            s_checksUnnamed[function] = checks;
            any |= checks;
        }

        return any;
    }

    // The languages of PostgreSQL's internal functions, of C functions and of SQL functions.
    private const uint InternalLanguage = 12;
    private const uint CLanguage = 13;
    private const uint SqlLanguage = 14;

    // The sources of PostgreSQL's internal functions that check a value against a type given as a value.
    private static readonly HashSet<string> s_inputSources = ["pg_input_is_valid", "pg_input_error_info", "record_in", "record_recv", "domain_in",
        "domain_recv", "array_in", "array_recv", "range_in", "range_recv", "multirange_in", "multirange_recv"];

    // A language's name, or null for one that is gone.
    private static unsafe string? LanguageName(uint language)
    {
        sbyte* name = NativeMethods.get_language_name(language, true);
        return name is null ? null : Worf.FromServer(name);
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
    /// Gets an expression for the node a draw from sequence <c>r.seqoid</c> waits for, or null once none. After this
    /// server's start: the node that granted the block, when another node did and this node knows it, until it has
    /// checked this node's report since the start; else a neighbor that reports the sequence and hasn't checked it since;
    /// else a neighbor whose message since the start hasn't been taken in whole and that reports nothing of the sequence.
    /// The neighbors are the nodes this node subscribes to, those that call it, and those its other neighbors say it has
    /// but it didn't see leave, as one restored from a backup taken before a node first called it knows of that node only
    /// from them. And at any time, the neighbor <see cref="Registering"/> gives. It reads <c>l</c> as
    /// worf.local_node, and its parameters are the start, the neighbors whose messages were taken in whole since, and the
    /// neighbors this transaction dropped.
    /// </summary>
    internal static string Unanswered(string grantor, string started, string complete, string dropped) => $"""
        coalesce(
            CASE WHEN {grantor} <> l.node_id AND (EXISTS (SELECT 1 FROM worf.node n WHERE n.node_id = {grantor})
                    OR EXISTS (SELECT 1 FROM worf.sequence_range_link k WHERE k.node_id = {grantor}))
                AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_node c WHERE c.seqoid = r.seqoid AND c.node_id = {grantor} AND c.confirmed_start = {started})
                THEN {grantor} END,
            (SELECT c.node_id FROM worf.sequence_range_node c
            WHERE c.seqoid = r.seqoid AND c.node_id <> l.node_id AND c.confirmed_start IS DISTINCT FROM {started}
                AND (EXISTS (SELECT 1 FROM worf.subscription s WHERE s.sub_origin = c.node_id AND s.sub_target = l.node_id)
                    OR EXISTS (SELECT 1 FROM worf.sequence_range_link k WHERE k.node_id = c.node_id)
                    OR EXISTS (SELECT 1 FROM worf.sequence_range_named m WHERE m.neighbor = c.node_id
                        AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_left d WHERE d.neighbor = m.neighbor))
                    OR c.node_id::pg_catalog.int8 = ANY ({dropped}))
            ORDER BY c.node_name LIMIT 1),
            (SELECT k.node_id FROM (
                    SELECT s.sub_origin AS node_id FROM worf.subscription s WHERE s.sub_target = l.node_id
                    UNION SELECT k.node_id FROM worf.sequence_range_link k
                    UNION SELECT m.neighbor FROM worf.sequence_range_named m
                        WHERE NOT EXISTS (SELECT 1 FROM worf.sequence_range_left d WHERE d.neighbor = m.neighbor)
                    UNION SELECT d::pg_catalog.oid FROM pg_catalog.unnest({dropped}::pg_catalog.int8[]) d) k
            WHERE k.node_id <> l.node_id AND NOT (k.node_id::pg_catalog.int8 = ANY ({complete}))
                AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_node c WHERE c.seqoid = r.seqoid AND c.node_id = k.node_id)
            ORDER BY k.node_id LIMIT 1),
            {Registering()})
        """;

    /// <summary>
    /// Gets an expression for the neighbor a draw from sequence <c>r.seqoid</c> waits for while another neighbor isn't
    /// yet one that a neighbor says this node has: one that exchanges with this node, its parent first, so that a
    /// restore can learn of every neighbor that may hold its values; or null. It reads <c>l</c> as worf.local_node.
    /// </summary>
    private static string Registering() => """
        (SELECT o.node_id FROM (
                SELECT s.sub_origin AS node_id FROM worf.subscription s WHERE s.sub_target = l.node_id
                UNION SELECT k.node_id FROM worf.sequence_range_link k) o
        WHERE o.node_id <> l.node_id
            AND EXISTS (SELECT 1 FROM worf.sequence_range_node c WHERE c.node_id = o.node_id AND c.via_node = c.node_id)
            AND EXISTS (
            SELECT 1 FROM (
                    SELECT s.sub_origin AS node_id FROM worf.subscription s WHERE s.sub_target = l.node_id
                    UNION SELECT k.node_id FROM worf.sequence_range_link k) u
            WHERE u.node_id <> o.node_id AND u.node_id <> l.node_id
                AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_named m WHERE m.neighbor = u.node_id))
        ORDER BY o.node_id = r.parent_node DESC, o.node_id LIMIT 1)
        """;

    /// <summary>
    /// Checks whether a sequence's draws after this server's start may go on without the nodes they wait for, as
    /// <c>worf.sequence_range_restart_wait</c> lets them once that long has passed since the start.
    /// </summary>
    internal static bool StartWaitOver => !WaitsUntilAnswered && Worker.GetCurrentTimestamp() >= SequenceRangeHeard.Started + (SequenceRanges.RestartWait * 1000L);

    // How long, in microseconds, a statement waits for a sequence's next block: twice worf.sequence_range_timeout, and,
    // where other nodes share the sequence, worf.sequence_range_offline_wait on top, after which a node that can't reach
    // them takes the block from its reserve, or, the allocator, switches to its own block without them.
    private static long BlockWait(uint seqoid)
        => (SequenceRanges.ExchangeTimeout * 2000L) + (SequenceRanges.OfflineWait > 0 && SharedWithOthers(seqoid) ? SequenceRanges.OfflineWait * 1000L : 0);

    // Whether other nodes share a sequence, as last committed: another node owns it, or reports it. worf's tables
    // are read straight, as ReadBlocks reads them, at every wait for a block.
    private static bool SharedWithOthers(uint seqoid)
    {
        unsafe
        {
            NativeMethods.CommandCounterIncrement();
            NativeMethods.PushActiveSnapshot(NativeMethods.GetLatestSnapshot());
        }

        try
        {
            List<object?[]> local = Worf.ScanCatalog("local_node", "node_id");
            List<object?[]> ranges = local.Count == 0 ? [] : Worf.ScanCatalog("sequence_range", "seqoid", "owner_node").FindAll(range => (uint)range[0]! == seqoid);
            List<object?[]> reports = ranges.Count == 0 ? [] : Worf.ScanCatalog("sequence_range_node", "seqoid", "node_id").FindAll(report => (uint)report[0]! == seqoid);
            return local.Exists(node => ranges.Exists(range => (uint)range[1]! != (uint)node[0]!) || reports.Exists(report => (uint)report[1]! != (uint)node[0]!));
        }
        finally
        {
            unsafe
            {
                NativeMethods.PopActiveSnapshot();
            }
        }
    }

    // Whether statements wait, after this server's start, for its file of starts and for the nodes they wait for until
    // those answer, failing after twice worf.sequence_range_timeout each, as they do unless
    // worf.sequence_range_restart_wait lets them draw once it has passed since the start.
    private static bool WaitsUntilAnswered => SequenceRanges.RestartWait < 0;

    // When a statement stops waiting for the file of starts or for a node: twice worf.sequence_range_timeout after it
    // starts to wait, or, unless statements wait until answered, worf.sequence_range_restart_wait after the start.
    private static long StartWaitEnds() => WaitsUntilAnswered
        ? Worker.GetCurrentTimestamp() + (SequenceRanges.ExchangeTimeout * 2000L)
        : SequenceRangeHeard.Started + (SequenceRanges.RestartWait * 1000L);

    /// <summary>
    /// Waits, before a statement draws from a managed sequence, until the data directory's file of starts keeps this
    /// server's start, which worf's supervisor notes again until it does: a later start that finds it gone, as after a
    /// restore whose start the file couldn't keep, would take no position from before it as gone back, and so draw again
    /// values drawn since. The statement lets its gates go and waits as for a neighbor after a start: up to twice
    /// <c>worf.sequence_range_timeout</c>, and then fails, or, when <c>worf.sequence_range_restart_wait</c> is set, until
    /// that has passed since the server started, and then draws, with a warning.
    /// </summary>
    private static void WaitForStartKept(uint seqoid, bool unnamed)
    {
        if (s_startKept || (s_startKept = SequenceRangeHeard.StartKept) || s_startUnkeptWarned)
        {
            return;
        }

        string sequence;
        unsafe
        {
            sequence = Worf.FromServer(NativeMethods.get_rel_name(seqoid));
        }

        using PgErrorContext context = PgErrorContext.Push("waiting for the file worf_starts in the data directory to keep this server's start");
        List<(uint Seqoid, bool Unit)> held = LetGates();
        try
        {
            long deadline = StartWaitEnds();
            while (!(s_startKept = SequenceRangeHeard.StartKept))
            {
                if (Worker.GetCurrentTimestamp() >= deadline && WaitsUntilAnswered)
                {
                    throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState,
                        unnamed ? "nextval can't draw until the file worf_starts in the data directory keeps this server's start"
                            : $"sequence {sequence} can't draw until the file worf_starts in the data directory keeps this server's start",
                        detail: "A server whose start the file doesn't keep can't tell, at its next start, whether it went back to before it, as a restored one does, and so whether it draws again values drawn since.",
                        hint: "Make worf_starts, and worf_starts.tmp if it is there, writable by the server; worf's supervisor keeps the start as soon as it can.");
                }

                if (Worker.GetCurrentTimestamp() >= deadline)
                {
                    s_startUnkeptWarned = true;
                    WorfLog.Warning(new PgDiagnostic(unnamed ? "nextval draws though the file worf_starts in the data directory doesn't keep this server's start"
                        : $"sequence {sequence} draws though the file worf_starts in the data directory doesn't keep this server's start")
                    {
                        Detail = "A server whose start the file doesn't keep can't tell, at its next start, whether it went back to before it, as a restored one does.",
                        Hint = "Make worf_starts, and worf_starts.tmp if it is there, writable by the server; worf's supervisor keeps the start as soon as it can.",
                    });
                    return;
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

    /// <summary>
    /// Waits, before a statement draws from a sequence's active block, until the block's parent, when another node
    /// granted it, and every other node this node exchanges with directly that reports the sequence have checked this
    /// node's report of it since this server started: a node that was away while its parent's fence lifted without its
    /// report, as when <c>worf.activate_sequence_range</c> stopped waiting for it, holds values the lift gave out again,
    /// and a node restored from a backup may draw again values a neighbor heard it draw, its own blocks' too, as an
    /// allocator's; the first message after from such a node asks it to reset, so it draws none of them. The statement
    /// lets its gates go and waits up to twice <c>worf.sequence_range_timeout</c>, and then fails, naming the parent, as an
    /// error that ends the wait first, as <c>statement_timeout</c>'s, does; when <c>worf.sequence_range_restart_wait</c>
    /// is set, it waits until that has passed since the server started, and then draws, with a warning.
    /// </summary>
    private static void WaitForGrantor(uint seqoid, bool unnamed = false)
    {
        WaitForStartKept(seqoid, unnamed);
        Reload();
        if (!s_grantors.TryGetValue(seqoid, out uint grantor) || s_confirmedSequences.Contains(seqoid) || s_unanswered.Contains((seqoid, grantor)))
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
                        (SELECT max(node_name::text) FROM worf.sequence_range_node WHERE node_id = $1),
                        (SELECT max(neighbor_name::text) FROM worf.sequence_range_named WHERE neighbor = $1))
                    """, SpiParameter.Create(waitFor))) ?? s_droppedNeighbors.GetValueOrDefault(waitFor) ?? waitFor.ToString(CultureInfo.InvariantCulture);
                heardBy = s_heardBy.Contains(seqoid);
                role = heardBy ? $"which may have heard of sequence {sequence}'s draws" : $"which granted sequence {sequence}'s block";
            }

            Describe(grantor);
            string waitingFor = $"waiting for node {parent}, {role}, to answer this server since it started" +
                (unnamed ? ", as worf can't tell which sequence the call of nextval draws from before it runs" : "");
            using PgErrorContext context = PgErrorContext.Push(Worf.Shown(waitingFor));
            List<(uint Seqoid, bool Unit)> held = LetGates();
            try
            {
                long deadline = StartWaitEnds();
                long woken = 0;
                while (true)
                {
                    // A neighbor's message taken in whole since, which changes no table, as one that reports no such
                    // sequence, ends the wait for it.
                    Refresh();
                    if (!SequenceRangeHeard.CompleteSince(SequenceRangeHeard.Started).SequenceEqual(s_complete))
                    {
                        s_version = -1;
                    }

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

                    // The wait makes the exchange with the node waited for due, each second while it lasts, and asks this
                    // node's subscribers to call it, as one that only calls this node can't be called.
                    long now = Worker.GetCurrentTimestamp();
                    if (now >= deadline && WaitsUntilAnswered)
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

                    if (now >= deadline)
                    {
                        _ = s_unanswered.Add((seqoid, current));
                        WorfLog.Warning(new PgDiagnostic($"sequence {sequence} draws though node {parent} hasn't answered this server since it started")
                        {
                            Detail = "If this server was restored from a backup, it may issue again values it issued after the backup, which that node may hold.",
                            Hint = "After restoring a node from a backup, run worf.reset_sequence_range() on it before it takes writes.",
                        });
                        break;
                    }

                    if (now - woken >= 1_000_000)
                    {
                        woken = now;
                        SequenceRangeHeard.Want(grantor);
                        SequenceRangeExchangeWorker.Wake();
                        Worker.WantWord();
                        FailIfWorfDropWaits(seqoid);
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
    /// <param name="draws">How many values the next row may draw, which the block must hold past its last twentieth.</param>
    private static bool Short(uint seqoid, bool ascending, long lo, long hi, long? position, Func<long?> reread, long draws = 1)
    {
        // A row that draws several values needs them all past the last twentieth, in the sequence's steps, up to as many
        // as a block of this size holds past its last twentieth, as one that draws more runs into the next however soon
        // the switch comes.
        decimal step = draws > 1 ? Step(seqoid) : 1;
        draws = Math.Min(draws, Math.Max((long)(((decimal)hi - lo) / step * 19 / 20), 1));
        decimal tail = (s_reservedActive.Contains((seqoid, lo)) ? 0 : ((decimal)hi - lo) / 20) + ((Math.Max(draws, 1) - 1) * step);
        decimal promised = PromisedElsewhere(seqoid) * Step(seqoid);
        decimal Left(long? at) => (at is long p ? (ascending ? (decimal)hi - p : (decimal)p - lo) : (decimal)hi - lo + 1) - promised;

        // No more statements can draw than there are backends, so a block with as many values left needs no count.
        decimal left = Left(position);
        if (left <= tail || left >= tail + Backends())
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
            return PagePosition(rel);
        }
        finally
        {
            NativeMethods.relation_close(rel, (int)PgLockMode.None);
        }
    }

    // Reads an open sequence's last value from its one page, as nextval reads it, or null when it hasn't issued
    // one: the sequence's one row holds its last value, its count of values logged ahead and whether it was
    // called, in that order. A scan of the sequence for the row costs several times as much, which every statement
    // that draws once would pay.
    private static unsafe long? PagePosition(RelationData* sequence)
    {
        // BUFFER_LOCK_SHARE from storage/bufmgr.h, which PostgreSQL 19 names in a type of its own, and
        // FirstOffsetNumber from storage/off.h.
#if ANKUS_PG19
        const BufferLockMode Share = BufferLockMode.BUFFER_LOCK_SHARE;
#else
        const int Share = 1;
#endif
        const ushort First = 1;
        int buffer = NativeMethods.ReadBuffer(sequence, 0);
        NativeMethods.LockBuffer(buffer, Share);
        try
        {
            sbyte* page = NativeMethods.BufferGetPage(buffer);
            HeapTupleHeaderData* row = (HeapTupleHeaderData*)NativeMethods.PageGetItem(page, NativeMethods.PageGetItemId(page, First));
            byte* values = (byte*)row + row->t_hoff;
            return values[2 * sizeof(long)] != 0 ? *(long*)values : null;
        }
        finally
        {
            NativeMethods.UnlockReleaseBuffer(buffer);
        }
    }

    /// <summary>
    /// Checks whether a table has the guard's triggers, from the system catalogs alone, so that a statement that writes
    /// to a table no managed sequence numbers reads none of worf's tables, as of the table's last change, as a partition
    /// attached, which takes on the guard.
    /// </summary>
    private static bool Guarded(uint relid) => Triggers(relid).Guarded;

    // The version of the blocks, read from its atomic, which costs a statement that runs a moment as much as a table
    // lookup, only once the counter the fast draws read has moved: a change moves the version first and that counter
    // straight after.
    private static long s_blocksVersion = -1;
    private static long s_blocksVersionAt = -1;

    private static unsafe long BlocksVersion()
    {
        long changes = Volatile.Read(ref Shared()[WorfFastDraws.Changes]);
        if (changes != s_blocksVersionAt || s_blocksVersion < 0)
        {
            s_blocksVersion = SequenceRanges.Version.Value;
            s_blocksVersionAt = changes;
        }

        return s_blocksVersion;
    }

    /// <summary>
    /// Checks, from the system catalogs alone, as <see cref="Guarded"/> does, whether a table has the guard's triggers,
    /// and whether it has a trigger that takes arguments, which may name a sequence the trigger draws from.
    /// </summary>
    private static (bool Guarded, bool Argued) Triggers(uint relid)
    {
        WatchTables();
        (long, long) version = (BlocksVersion(), SequenceRanges.LocalVersion);
        if (version != s_guardedAt)
        {
            s_guarded.Clear();
            s_checked.Clear();
            s_guardedAt = version;
        }

        if (!s_guarded.TryGetValue(relid, out (bool Guarded, bool Argued) triggers))
        {
            if (CachedTriggers(relid) is (bool, bool) cached)
            {
                s_guarded[relid] = cached;
                return cached;
            }

            List<(uint Table, string Name, uint Function, int Type, char Enabled, bool Internal, string[] Arguments, string? Condition)> found = TreeTriggers(relid);
            triggers = (found.Exists(trigger => trigger.Table == relid && HooksGuard(trigger.Function)),
                found.Exists(static trigger => !trigger.Internal && trigger.Enabled != 'D' && trigger.Arguments.Length > 0));
            s_guarded[relid] = triggers;
        }

        return triggers;
    }

    // Reads, for an ordinary table, whether the guard checks it and whether a trigger of it takes arguments, from the
    // table's own entry in PostgreSQL's cache. Null for a partitioned table, whose partitions' triggers count too.
    private static unsafe (bool Guarded, bool Argued)? CachedTriggers(uint relid)
    {
        const sbyte Disabled = (sbyte)'D';
        RelationData* table = NativeMethods.relation_open(relid, (int)PgLockMode.None);
        try
        {
            if (table->rd_rel->relkind != (sbyte)'r')
            {
                return null;
            }

            (bool guarded, bool argued) = (false, false);
            for (int i = 0; table->trigdesc is not null && i < table->trigdesc->numtriggers; i++)
            {
                Trigger* trigger = &table->trigdesc->triggers[i];
                guarded |= HooksGuard(trigger->tgfoid);
                argued |= !trigger->tgisinternal && trigger->tgenabled != Disabled && trigger->tgnargs > 0;
            }

            return (guarded, argued);
        }
        finally
        {
            NativeMethods.relation_close(table, (int)PgLockMode.None);
        }
    }

    // Reads the arguments of an ordinary table's triggers that fire on some events in this session, with each
    // trigger's function, from the table's own entry in PostgreSQL's cache. Null for a partitioned table.
    private static unsafe List<(uint Function, string Argument)>? CachedArguments(uint relid, int events, bool replica)
    {
        const sbyte Always = (sbyte)'A';
        sbyte fires = replica ? (sbyte)'R' : (sbyte)'O';
        RelationData* table = NativeMethods.relation_open(relid, (int)PgLockMode.None);
        try
        {
            if (table->rd_rel->relkind != (sbyte)'r')
            {
                return null;
            }

            List<(uint Function, string Argument)> arguments = [];
            for (int i = 0; table->trigdesc is not null && i < table->trigdesc->numtriggers; i++)
            {
                Trigger* trigger = &table->trigdesc->triggers[i];
                if (trigger->tgisinternal || trigger->tgnargs <= 0 || (trigger->tgenabled != Always && trigger->tgenabled != fires) || (trigger->tgtype & events) == 0)
                {
                    continue;
                }

                for (int at = 0; at < trigger->tgnargs; at++)
                {
                    string argument = Worf.FromServer(trigger->tgargs[at]);
                    if (argument.Length > 0 && !arguments.Contains((trigger->tgfoid, argument)))
                    {
                        arguments.Add((trigger->tgfoid, argument));
                    }
                }
            }

            return arguments;
        }
        finally
        {
            NativeMethods.relation_close(table, (int)PgLockMode.None);
        }
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

        WatchTables();
        if (!s_drawnBy.TryGetValue(relid, out List<uint>? sequences))
        {
            sequences = CachedDrawnBy(relid);
            s_drawnBy[relid] = sequences;
        }

        // The insert triggers that fire depend on the session's replication role.
        return [.. sequences, .. ArguedBy(relid, TriggerTypeInsert).Where(seqoid => !sequences.Contains(seqoid))];
    }

    // Reads the managed sequences a table depends on through its columns' defaults, or owns for its identity
    // columns, straight from pg_depend: a query for them would leave a session what PostgreSQL looked up to plan
    // it. A partition's identity columns draw from the sequences of the tables above it, from PostgreSQL 17 on.
    private static unsafe List<uint> CachedDrawnBy(uint relid)
    {
        // pg_depend, its indexes by dependent and by referenced object, pg_attrdef and pg_class, and pg_depend's
        // columns classid, objid, refclassid, refobjid and deptype, which every supported version numbers so.
        const uint Depend = 2608;
        const uint ByDependent = 2673;
        const uint ByReferenced = 2674;
        const uint Defaults = 2604;
        const uint Classes = 1259;
        HashSet<uint> found = [];
        RelationData* table = NativeMethods.try_relation_open(relid, (int)PgLockMode.None);
        if (table is null)
        {
            return [];
        }

        try
        {
            // Before PostgreSQL 17 a partition shares no identity column with its ancestors: an insert into it
            // draws from its own identity column's sequence, if it has one.
            TupleDescData* row = table->rd_att;
            for (int i = 0; row->constr is not null && i < row->constr->num_defval; i++)
            {
                uint value = NativeMethods.GetAttrDefaultOid(relid, row->constr->defval[i].adnum);
                found.UnionWith(Worf.ScanRelation(Depend, ByDependent, [(1, Defaults), (2, value)], false, 4, 5)
                    .Where(static used => (uint)used[0]! == Classes).Select(static used => (uint)used[1]!));
            }

            bool identity = false;
            for (int i = 0; i < row->natts && !identity; i++)
            {
                FormData_pg_attribute* column = NativeMethods.TupleDescAttr(row, i);
                identity = !column->attisdropped && column->attidentity != 0;
            }

            List<object?[]> ownedBy = identity ? Worf.ScanRelation(Depend, ByReferenced, [(4, Classes), (5, relid)], false, 1, 2, 7) : [];
            found.UnionWith(ownedBy.Where(static owned => (uint)owned[0]! == Classes && (char)owned[2]! == 'i').Select(static owned => (uint)owned[1]!));

#if !(ANKUS_PG15 || ANKUS_PG16)
            List* above = identity && table->rd_rel->relispartition ? NativeMethods.get_partition_ancestors(relid) : null;
            for (int at = 0; above is not null && at < above->length; at++)
            {
                uint ancestor = above->elements[at].oid_value;
                HashSet<long> columns = [];
                for (int i = 0; i < row->natts; i++)
                {
                    FormData_pg_attribute* column = NativeMethods.TupleDescAttr(row, i);
                    if (!column->attisdropped && column->attidentity != 0 && NativeMethods.get_attnum(ancestor, (sbyte*)&column->attname) is > 0 and short named)
                    {
                        _ = columns.Add(named);
                    }
                }

                found.UnionWith(Worf.ScanRelation(Depend, ByReferenced, [(4, Classes), (5, ancestor)], false, 1, 2, 7, 6)
                    .Where(owned => (uint)owned[0]! == Classes && (char)owned[2]! == 'i' && columns.Contains((long)owned[3]!))
                    .Select(static owned => (uint)owned[1]!));
            }
#endif
        }
        finally
        {
            NativeMethods.relation_close(table, (int)PgLockMode.None);
        }

        return [.. found.Where(s_sequences.ContainsKey)];
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
        bool replica = Replica();
        if (!s_arguedBy.TryGetValue((relid, events, movedInto, replica), out List<uint>? sequences))
        {
            sequences = [.. ArguedFunctions(relid, events, movedInto).Values.SelectMany(named => named).Distinct()];
            s_arguedBy[(relid, events, movedInto, replica)] = sequences;
        }

        return sequences;
    }

    // Whether this session applies changes as a replica, where only the triggers enabled ALWAYS or REPLICA fire, rather
    // than those enabled ALWAYS or ORIGIN.
    private static bool Replica()
    {
        unsafe
        {
            return NativeGlobals.SessionReplicationRole == SessionReplicationRoleReplica;
        }
    }

    // The trigger functions of the triggers of a table, and its partitions, that fire on some events, as ArguedBy reads
    // them, in this session's replication role, with the managed sequences each one's triggers name among their arguments.
    private static Dictionary<uint, List<uint>> ArguedFunctions(uint relid, int events, int movedInto = 0)
    {
        WatchTables();
        bool replica = Replica();
        if (!s_arguedFunctions.TryGetValue((relid, events, movedInto, replica), out Dictionary<uint, List<uint>>? functions))
        {
            // An ordinary table's are read from PostgreSQL's cache, which the statement that writes it has loaded.
            List<(uint Function, string Argument)>? given = CachedArguments(relid, events, replica);
            if (given is null)
            {
                char fires = replica ? 'R' : 'O';
                given = [.. TreeTriggers(relid)
                    .Where(trigger => !trigger.Internal && (trigger.Enabled == 'A' || trigger.Enabled == fires))
                    .Where(trigger => (trigger.Type & events) != 0 || (trigger.Table != relid && (trigger.Type & movedInto) != 0))
                    .SelectMany(static trigger => trigger.Arguments.Where(static argument => argument.Length > 0).Select(argument => (trigger.Function, argument)))
                    .Distinct()];
            }

            functions = [];
            foreach ((uint function, string argument) in given)
            {
                if (ManagedNamed(argument) is HashSet<uint> named && named.Count > 0)
                {
                    functions[function] = [.. functions.GetValueOrDefault(function, []).Union(named)];
                }
            }

            s_arguedFunctions[(relid, events, movedInto, replica)] = functions;
        }

        return functions;
    }

    /// <summary>
    /// Takes the gates of a statement's sequences that this backend doesn't hold, in the sequences' order, and hears of
    /// the switches committed while it waited for them, so that the statement's first nextval reads their bounds.
    /// </summary>
    private static void OpenGates(nint statement, List<uint> sequences)
    {
        // A gate a unit holds around the statement, as a row's whose function runs the statement, becomes the
        // statement's own, still held, until the statement ends, when it is the unit's again.
        uint subxact;
        unsafe
        {
            subxact = NativeMethods.GetCurrentSubTransactionId();
        }

        List<uint> taken = [];
        foreach (uint seqoid in sequences.Distinct().Order().Where(seqoid => !s_gates.ContainsKey(seqoid) || s_unitGates.Contains(seqoid)))
        {
            if (s_unitGates.Remove(seqoid))
            {
                s_gates[seqoid] = subxact;
                CountDrawer(seqoid, 1);
            }
            else
            {
                TakeGate(seqoid);
            }

            taken.Add(seqoid);
        }

        if (taken.Count > 0)
        {
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
                // A gate the unit around the statement took before it is the unit's again, still held, so that no switch
                // moves the sequence before the unit's own draws.
                if (s_gates.ContainsKey(seqoid) && UnitTook(seqoid))
                {
                    _ = s_unitGates.Add(seqoid);
                    CountDrawer(seqoid, -1);
                }
                else
                {
                    LetGate(seqoid);
                }

                _ = s_gatedRowDraws.Remove(seqoid);
                _ = s_unseenRowDraws.Remove(seqoid);
                _ = s_rowMarks.Remove(seqoid);
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

        List<(uint Seqoid, bool Unit)> passing = [.. s_gates.Keys.Where(seqoid => Drawer(seqoid).Switching > 0).Order()
            .Select(seqoid => (seqoid, s_unitGates.Contains(seqoid)))];
        foreach ((uint seqoid, _) in passing)
        {
            LetGate(seqoid);
        }

        TakeGates(passing);
        return [.. passing.Select(gate => gate.Seqoid)];
    }

    /// <summary>
    /// Lets every gate this backend holds go, as before it waits for a switch, which waits for the gates.
    /// </summary>
    /// <returns>The gates it held, and whether each was a unit's.</returns>
    private static List<(uint Seqoid, bool Unit)> LetGates()
    {
        List<(uint Seqoid, bool Unit)> held = [.. s_gates.Keys.Order().Select(seqoid => (seqoid, s_unitGates.Contains(seqoid)))];
        foreach ((uint seqoid, _) in held)
        {
            LetGate(seqoid);
        }

        return held;
    }

    /// <summary>
    /// Takes gates again, and hears of the switches committed meanwhile.
    /// </summary>
    private static void TakeGates(List<(uint Seqoid, bool Unit)> gates)
    {
        foreach ((uint seqoid, bool unit) in gates)
        {
            TakeGate(seqoid, unit);
        }

        if (gates.Count > 0)
        {
            Refresh();
        }
    }

    // Takes a sequence's gate, for a statement that writes, which counts among those drawing from the sequence while it
    // holds it, or for a unit of one that doesn't, which its statement's own count covers.
    private static unsafe void TakeGate(uint seqoid, bool unit = false)
    {
        HookExit();
        RegisterGates();

        // The gate is free unless a switch holds it or waits for it, and then the door, which the switch shuts, is waited
        // at first, as for a lock, until the switch has committed.
        if (AcquireGate(seqoid, ShareLock, true) == LockAcquireResult.LOCKACQUIRE_NOT_AVAIL)
        {
            LOCKTAG door = GateTag(seqoid, DoorKey);
            _ = NativeMethods.LockAcquire(&door, ShareLock, false, false);
            _ = AcquireGate(seqoid, ShareLock, false);
            _ = NativeMethods.LockRelease(&door, ShareLock, false);
        }

        // The gate is noted once what counts it has succeeded, so that a failure leaves nothing to count down.
        uint subxact = NativeMethods.GetCurrentSubTransactionId();
        if (!unit)
        {
            CountDrawer(seqoid, 1);
        }

        s_gates[seqoid] = subxact;
        if (unit)
        {
            _ = s_unitGates.Add(seqoid);
            WatchStatements();
        }
    }

    private static void LetGate(uint seqoid)
    {
        if (s_gates.ContainsKey(seqoid))
        {
            // A release that fails, as every call into PostgreSQL does while a cancellation is pending, leaves the gate
            // noted, for the transaction's end, which lets its lock go, to count down.
            ReleaseGate(seqoid, ShareLock);
            _ = s_gates.Remove(seqoid);
            if (s_unitGates.Remove(seqoid))
            {
                WatchStatements();
            }
            else
            {
                CountDrawer(seqoid, -1);
            }
        }
    }

    // Takes a sequence's gate, as a statement does or a switch, under the transaction's own resource owner, whichever
    // portal's or subtransaction's is current: PostgreSQL lets a lock go only under the owner that took it, and a gate
    // taken as a cursor's row runs is let go as the loop over the cursor runs, under another.
    private static unsafe LockAcquireResult AcquireGate(uint seqoid, int mode, bool nowait)
    {
        LOCKTAG tag = GateTag(seqoid);
        ResourceOwnerData* current = NativeGlobals.CurrentResourceOwner;
        NativeGlobals.CurrentResourceOwner = NativeGlobals.TopTransactionResourceOwner;
        try
        {
            return NativeMethods.LockAcquire(&tag, mode, false, nowait);
        }
        finally
        {
            NativeGlobals.CurrentResourceOwner = current;
        }
    }

    // Lets a sequence's gate go, under the resource owner AcquireGate took it under.
    private static unsafe void ReleaseGate(uint seqoid, int mode)
    {
        LOCKTAG tag = GateTag(seqoid);
        ResourceOwnerData* current = NativeGlobals.CurrentResourceOwner;
        NativeGlobals.CurrentResourceOwner = NativeGlobals.TopTransactionResourceOwner;
        try
        {
            _ = NativeMethods.LockRelease(&tag, mode, false);
        }
        finally
        {
            NativeGlobals.CurrentResourceOwner = current;
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
    private static void CountDrawer(uint seqoid, int statements, int waiting = 0, int switching = 0, long waitingFor = 0, long promised = 0)
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
                drawer.Promised = Math.Max(drawer.Promised + promised, 0);
                guard[i] = drawer;
                return;
            }

            if (free < 0 && (guard[i].Dboid == 0 || guard[i] is { Statements: 0, Waiting: 0, Switching: 0, Promised: 0 }))
            {
                free = i;
            }

            if (guard[i].Dboid == 0)
            {
                break;
            }
        }

        if (free >= 0 && (statements > 0 || waiting > 0 || switching > 0 || promised > 0))
        {
            guard[free] = new WorfDrawer
            {
                Dboid = dboid,
                Seqoid = seqoid,
                Statements = Math.Max(statements, 0),
                Promised = Math.Max(promised, 0),
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
        CountPending(1);
        CountDrawer(seqoid, 0, 0, 1);

        // Backends hear of the switch before it no longer counts as under way, so that a row that draws without the gate
        // reads the new bounds either way.
        void Done()
        {
            SequenceRanges.Changed();
            CountDrawer(seqoid, 0, 0, -1);
            CountPending(-1);
        }

        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, Done);
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, Done);
        _ = Worf.Execute($"SET LOCAL lock_timeout = {timeout.ToString(CultureInfo.InvariantCulture)}");
        LOCKTAG door = GateTag(seqoid, DoorKey);
        _ = NativeMethods.LockAcquire(&door, ExclusiveLock, false, false);
        LOCKTAG tag = GateTag(seqoid);
        _ = NativeMethods.LockAcquire(&tag, ExclusiveLock, false, false);
        AwaitFastDraws(timeout);
    }

    // Waits for the draws under way that hold no gate, which count themselves in shared memory for the moment each
    // takes: the switch has counted itself as pending, so no more of them start, and those that had are done within
    // that moment, unless one waits for a lock within nextval, which holds the switch up as a statement that holds
    // the gate does, for as long as the switch waits for a lock.
    private static unsafe void AwaitFastDraws(int timeout)
    {
        long* draws = Shared();
        long until = Worker.GetCurrentTimestamp() + (timeout * 1000L);
        for (int spins = 0; ; spins++)
        {
            bool drawing = false;
            int slots = (int)Math.Min(WorfFastDraws.Slots, NativeGlobals.ProcGlobal->allProcCount);
            for (int slot = 1; slot <= slots && !drawing; slot++)
            {
                drawing = Volatile.Read(ref draws[slot * WorfFastDraws.SlotLongs]) != 0;
            }

            if (!drawing)
            {
                return;
            }

            if (spins < 100)
            {
                Thread.SpinWait(100);
                continue;
            }

            PgInterrupts.Check();
            if (Worker.GetCurrentTimestamp() >= until)
            {
                throw new PgException("55P03", "canceling statement due to lock timeout");
            }

            Worker.WaitLatch(1);
        }
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
        // The switch counts as pending no more once the wait is over, or once the transaction ends where the wait
        // failed: a cancellation, which stays pending until the statement ends, fails every call into PostgreSQL
        // after it, the shared table's lock too.
        bool pending = false;
        bool switching = false;
        void Counted()
        {
            if (pending)
            {
                pending = false;
                CountPending(-1);
            }

            if (switching)
            {
                CountDrawer(seqoid, 0, 0, -1);
                switching = false;
            }
        }

        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, Counted);
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, Counted);
        CountPending(1);
        pending = true;
        CountDrawer(seqoid, 0, 0, 1);
        switching = true;
        int nestLevel = NativeMethods.NewGUCNestLevel();
        LOCKTAG door = GateTag(seqoid, DoorKey);
        LOCKTAG tag = GateTag(seqoid);
        bool shut = false;
        bool taken = false;
        try
        {
            WorfLocks.SetAtNestLevel("lock_timeout", "0");
            WorfLocks.SetAtNestLevel("deadlock_timeout", "1h");
            _ = NativeMethods.LockAcquire(&door, ExclusiveLock, false, false);
            shut = true;
            _ = NativeMethods.LockAcquire(&tag, ExclusiveLock, false, false);
            taken = true;
        }
        finally
        {
            try
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
            }
            finally
            {
                Counted();
            }
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
    private static void RegisterGates() => RegisterEnd();

    // Passes the gates a subtransaction took to its parent as it commits.
    private static void PassGatesToParent(uint subxact, uint parent)
    {
        foreach (uint seqoid in s_gates.Where(gate => gate.Value == subxact).Select(gate => gate.Key).ToList())
        {
            s_gates[seqoid] = parent;
        }

        for (int i = 0; i < s_gating.Count; i++)
        {
            if (s_gating[i].Subxact == subxact)
            {
                s_gating[i] = s_gating[i] with { Subxact = parent };
            }
        }
    }

    // Lets go of the gates a subtransaction that aborts took: its statements end without ExecutorEnd, and its PL/pgSQL
    // statements without their ends. The gates are the transaction's own, so PostgreSQL doesn't let them go with it.
    private static void ForgetGatesOf(uint aborted)
    {
        foreach (uint seqoid in s_gates.Where(gate => gate.Value == aborted).Select(gate => gate.Key).ToList())
        {
            _ = s_gates.Remove(seqoid);
            ReleaseGate(seqoid, ShareLock);
            if (s_unitGates.Remove(seqoid))
            {
                continue;
            }

            _ = s_gatedRowDraws.Remove(seqoid);
            _ = s_unseenRowDraws.Remove(seqoid);
            _ = s_rowMarks.Remove(seqoid);
            CountDrawer(seqoid, -1);
        }

        // A draw that failed within a bracket's steps is under way no more, and a statement that failed, which the
        // guard knows only by where it stood, may be any of those that went by what the guard kept.
        LeaveFastDraw();
        s_settledRunning.Clear();
        s_settledStarting = default;
        _ = s_gating.RemoveAll(entry => entry.Subxact == aborted);
        int dead = s_scopes.FindIndex(scope => scope.Subxact != 0 && scope.Subxact >= aborted);
        if (dead >= 0)
        {
            CloseScopes(dead, false);
        }

        ForgetSitesOf(aborted);
    }

    private static void ForgetGates()
    {
        s_rowsDrawing.Clear();
        foreach (uint seqoid in s_gates.Keys.Where(seqoid => !s_unitGates.Contains(seqoid)))
        {
            CountDrawer(seqoid, -1);
        }

        s_gates.Clear();
        s_unitGates.Clear();
        s_scopes.Clear();
        s_scopeGates.Clear();
        s_statementScopes = 0;

        // The states of the transaction's PL/pgSQL expressions end with it, and their calls' places may be others' next.
        s_startedSites.Clear();
        s_singleCalls.Clear();
        s_planSites.Clear();
        foreach (List<nint> made in s_brackets.Values)
        {
            foreach (nint bracket in made)
            {
                s_freeBrackets.Push(bracket);
            }
        }

        s_brackets.Clear();
        s_settledRunning.Clear();
        s_settledStarting = default;
        s_freeSites.Clear();
        if (s_sites.Count > 0)
        {
            s_sites.Clear();
            s_siteOf.Clear();
            s_siteGeneration++;
        }

        s_handedOff = 0;
        s_compiledCalls = false;
        LeaveFastDraw();
        WatchStatements();
        s_gatedRowDraws.Clear();
        s_unseenRowDraws.Clear();
        s_rowMarks.Clear();
        s_gating.Clear();
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
        // The first row a partition takes of a statement whose rows take the column's default runs the partition's
        // trigger once, before worf marks it skipped: its value is one the sequence has just issued.
        if (inserted && (s_routing.Count > 0 || s_copyRouting.State != 0) && ChecksDefaultTaken())
        {
            return false;
        }

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

        if (inserted)
        {
            SeeRowDraws(seqoid);
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
    /// check after it, and <c>worf.sequence_range_offline_wait</c> on top, after which a node that can't reach the others
    /// takes the block from its reserve, however many of its statements and rows wait. Once the next block is here, the switch is this
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
        List<(uint Seqoid, bool Unit)> held = LetGates();
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
        // A wait that a cancellation ends can't count itself down, as no call into PostgreSQL succeeds until the
        // statement has ended, so the transaction's end does.
        HookExit();
        if (change > 0)
        {
            RegisterEnd();
        }

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

            foreach (uint seqoid in s_gates.Keys.Where(seqoid => !s_unitGates.Contains(seqoid)))
            {
                CountDrawer(seqoid, -1);
            }

            foreach (uint seqoid in s_promised.Keys.ToList())
            {
                Promise(seqoid, 0);
            }

            LeaveFastDraw();
            s_waitingFor.Clear();
            s_gates.Clear();
            s_unitGates.Clear();
            s_gatedRowDraws.Clear();
            s_unseenRowDraws.Clear();
            s_rowMarks.Clear();
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
        long statement;
        unsafe
        {
            statement = NativeMethods.GetCurrentStatementStartTimestamp();
        }

        if (!s_waited.TryGetValue((seqoid, lo), out (long Deadline, bool Expired, long Statement) waited) || waited.Statement != statement)
        {
            waited = (now + BlockWait(seqoid), false, statement);
            s_waited[(seqoid, lo)] = waited;
        }

        // A wait for the block that ran out of time already in this client statement, as an earlier row's or an earlier
        // statement's of a function it runs, waits no more, unless the block has switched since.
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
                FailIfWorfDropWaits(seqoid);
                if (!Elevated.Run(() => Worf.SelectCatalog("""
                    SELECT EXISTS (SELECT 1 FROM worf.sequence_range WHERE seqoid = $1 AND state IN ('active', 'exhausted') AND stranded_at IS NULL)
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
                s_waited[(seqoid, lo)] = (deadline, true, statement);
                return false;
            }
        }
    }

    /// <summary>
    /// Holds off worf's drop for the transaction of a statement that writes a guarded table, as
    /// <see cref="HoldOffWorfDrop()"/> does, before the statement reads worf's tables. In a database without worf no
    /// table is guarded.
    /// </summary>
    /// <param name="written">The tables the statement writes.</param>
    internal static void HoldOffWorfDrop(List<uint> written)
    {
        if (written.Count > 0 && Worf.CatalogOid("sequence_range") != 0 && written.Exists(table => Triggers(table).Guarded))
        {
            HoldOffWorfDrop();
        }
    }

    /// <summary>
    /// Holds off worf's drop for the transaction of a statement that draws from a managed sequence into a guarded table,
    /// sharing the advisory lock the drop takes alone, or fails it at once while a drop holds the lock or waits for it,
    /// so that statements that keep starting don't keep taking the tables the drop waits for.
    /// </summary>
    private static unsafe void HoldOffWorfDrop()
    {
        LOCKTAG tag = WorfDropTag();
        if (NativeMethods.LockAcquire(&tag, ShareLock, false, true) == LockAcquireResult.LOCKACQUIRE_NOT_AVAIL && !HoldsWorfTable())
        {
            throw new PgException(PgSqlStates.ObjectInUse, "worf is being dropped");
        }
    }

    /// <summary>
    /// Checks whether this transaction holds one of worf's tables, as one that read them or reserved a block does, which
    /// the drop waits for as it takes them all at once, so that its statements go on: the drop can't end before it does.
    /// </summary>
    private static bool HoldsWorfTable()
        => Elevated.Run(() => Worf.ReadScalar<bool>("""
            SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks k
                JOIN pg_catalog.pg_depend d ON d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.objid = k.relation
                    AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND d.deptype = 'e'
                JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid AND e.extname = 'worf'
                WHERE k.pid = pg_catalog.pg_backend_pid() AND k.locktype = 'relation' AND k.granted
                    AND k.database = (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()))
            """));

    /// <summary>
    /// Takes, for worf's drop, the lock statements that draw into guarded tables share, alone, waiting for them as for a
    /// lock, until the transaction ends.
    /// </summary>
    internal static unsafe void TakeWorfDropLock()
    {
        LOCKTAG tag = WorfDropTag();
        _ = NativeMethods.LockAcquire(&tag, ExclusiveLock, false, false);
    }

    // Names the lock worf's drop takes alone and statements that draw into guarded tables share, an advisory lock of the
    // database's in the lock method worf's gates take, which pg_locks shows with its keys.
    private static LOCKTAG WorfDropTag() => new()
    {
        locktag_field1 = DatabaseId(),
        locktag_field2 = Executor.WorfDropLockKey,
        locktag_field3 = Executor.WorfDropLockId,
        locktag_field4 = 2,
        locktag_type = LockTagAdvisory,
        locktag_lockmethodid = DefaultLockMethod,
    };

    /// <summary>
    /// Fails a statement that waits for a sequence's block once a drop of worf is under way, as the advisory lock it
    /// holds or waits for says: the drop waits for the tables the statement holds, as one that read worf's tables or wrote a guarded
    /// table does until it ends, the workers that would bring the block queue behind the drop, and PostgreSQL's deadlock
    /// detector doesn't see a wait on a latch, so the statement and the drop would hold each other up until one timed
    /// out.
    /// </summary>
    internal static void FailIfWorfDropWaits(uint seqoid)
    {
        if (Elevated.Run(() => Worf.SelectCatalog("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_locks l
                WHERE l.locktype = 'advisory' AND l.database = $1 AND l.classid = $2 AND l.objid = $3 AND l.objsubid = 2
                    AND l.mode = 'ExclusiveLock' AND l.pid <> pg_catalog.pg_backend_pid())
            """, SpiParameter.Create(DatabaseId()), SpiParameter.Create((uint)Executor.WorfDropLockKey), SpiParameter.Create((uint)Executor.WorfDropLockId))[0].Get<bool>(0)))
        {
            throw new PgException(PgSqlStates.ObjectInUse,
                $"sequence {SequenceRanges.GetInfo(seqoid).Name} stops waiting for its next block, as worf is being dropped");
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

        List<(uint Seqoid, bool Unit)> held = LetGates();

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
        long deadline = Worker.GetCurrentTimestamp() + BlockWait(seqoid);
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

        if (((reserved ? last : WakeNearEnd(seqoid, ascending, lo, hi, value)) ||
                (!last && Short(seqoid, ascending, lo, hi, Position(seqoid) ?? value, () => Position(seqoid) ?? value, s_gatedRowDraws.GetValueOrDefault(seqoid, 1)))) &&
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
    /// <param name="condition">The text of the COPY's own WHERE condition, as <see cref="CopyConditionText"/> reads it.</param>
    /// <param name="columns">The columns the COPY reads, by number, or null for all of them.</param>
    /// <param name="defaults">Whether the COPY's DEFAULT option lets a marker stand for a column's default.</param>
    /// <returns>The condition, unparsed, or null when no managed sequence numbers the table's rows.</returns>
    internal static unsafe Node* CopyCondition(uint relid, string condition, short[]? columns, bool defaults)
    {
        s_copying.Add([]);
        s_copyGates.Add(--s_lastCopyGate);
        s_copyFirst.Add(default);
        s_copyTables.Add(relid);

        // A database without worf has none of its tables to read, and a table whose rows nothing works out or checks
        // draws from no sequence: the COPY then reads nothing of worf's, which the session would keep.
        if (relid == 0 || Worf.CatalogOid("sequence_range") == 0 || (condition.Length == 0 && TakesRowsAsRead(relid)))
        {
            return null;
        }

        Reload();
        if (s_sequences.Count == 0)
        {
            return null;
        }

        // The conditions of the triggers that fire after a row are worked out for a buffer of rows at once, where
        // COPY writes a buffer at a time: the first buffer's would all run before the guard could reach the calls
        // they start. So the COPY's first row has the conditions start, in the condition worf adds, which it then
        // drops, and the guard reaches their calls before the row after. Where the COPY has a condition of its own,
        // which can't be dropped, or the table has partitions, which take their rows later, the COPY writes its rows
        // one by one instead, as a condition that may change from row to row has it do.
        bool after = AfterConditionsDraw(relid);
        bool alone = condition.Length == 0;
        bool prepares = after && alone && NativeMethods.get_rel_relkind(relid) == (sbyte)'r' && CopyErrorCallback() != 0;
        Node* apart = after && !prepares ? Parsed("SELECT pg_catalog.random() >= 0") : null;

        // A COPY that reads none of the columns the guard's trigger checks, each of which then takes what its own
        // default draws, holds no gate and checks no sequence for each row: its first row has the defaults' calls go
        // through the guard one by one, as a statement's do, and its rows skip the trigger.
        List<uint> identities = [];
        short verifies = 0;
        bool takes = !defaults && CopyErrorCallback() != 0 && CopyTakesDefaults(relid, columns, identities, out verifies);
        s_copyIdentities = takes && identities.Count > 0 ? identities : null;
        if (!takes)
        {
            s_copying[^1].AddRange(DrawnBy(relid));
        }

        // The rows a COPY gives a partitioned table whose guard triggers hold no condition are checked as they are
        // read, in the COPY's condition, as a statement checks its own: the partitions' triggers don't fire here.
        string routed = takes ? "" : RoutedCheck(relid);
        if (routed.Length > 0)
        {
            alone = false;
        }

        // A row's values are checked against their columns' domains as the row is read, by expressions the input
        // functions keep to themselves, and the COPY's own condition runs for each row: the sequences those draw from
        // are checked for each row as the defaults' are. Where they are all the COPY checks, its first row looks
        // for the calls themselves, which then go through the guard one by one as a statement's do, unless the
        // condition calls a function that may check a value against a domain worf can't tell beforehand.
        string reads = CopyDefaultsText(relid, null, false) + " " + condition;
        (HashSet<uint> read, _, bool unnamed) = Draws(reads);
        uint[] besides = [.. (unnamed ? s_sequences.Keys : read.Where(s_sequences.ContainsKey)).Where(seqoid => !s_copying[^1].Contains(seqoid))];
        s_copying[^1].AddRange(besides);
        bool finds = besides.Length > 0 && besides.Length == s_copying[^1].Count && CopyErrorCallback() != 0 && !ChecksUnnamedDomains(reads);
        s_copyFirst[^1] = (prepares, finds, alone, takes, s_copyIdentities, verifies);

        // A table the guard checks has its first row see to the guard's triggers, whatever its defaults draw from.
        if (s_copying[^1].Count == 0 && !prepares && !takes && !Triggers(relid).Guarded)
        {
            return apart;
        }

        if (s_copying[^1].Count > 0)
        {
            OpenGates(s_copyGates[^1], s_copying[^1]);
            WaitBeforeDrawing(s_copying[^1]);
        }

        if (!s_copyCheckInstalled)
        {
            *GucOption.ShowHook("worf.sequence_range_copy_check") = CopyCheckShow;

            MemoryContextData* old = Worf.MemoryContextSwitchTo(NativeGlobals.TopMemoryContext);
            s_copyCheckValue = (nint)Worf.PStrDup("");
            _ = Worf.MemoryContextSwitchTo(old);
            s_copyCheckInstalled = true;
        }

        Node* check = Parsed("SELECT pg_catalog.current_setting('worf.sequence_range_copy_check') IS NOT NULL");
        List* terms = NativeMethods.lappend(null, check);
        if (routed.Length > 0)
        {
            terms = NativeMethods.lappend(terms, Parsed("SELECT " + routed));
        }

        if (apart is not null)
        {
            terms = NativeMethods.lappend(terms, apart);
        }

        return terms->length == 1 ? check : (Node*)NativeMethods.makeBoolExpr(BoolExprType.AND_EXPR, terms, -1);
    }

    // Whether a COPY writes a table's rows as it reads them, with nothing of the table's to start that could call
    // nextval: an ordinary table with no trigger, no default, identity or generated column and no CHECK constraint,
    // whose columns' types are base types that hold nothing else, as no domain, array, row or range does. The
    // table's own entry in PostgreSQL's cache tells, which the COPY has loaded, so worf reads no catalog for it.
    private static unsafe bool TakesRowsAsRead(uint relid)
    {
        RelationData* table = NativeMethods.relation_open(relid, (int)PgLockMode.None);
        try
        {
            TupleDescData* row = table->rd_att;
            if (table->rd_rel->relkind != (sbyte)'r' || table->trigdesc is not null ||
                (row->constr is not null && (row->constr->num_defval != 0 || row->constr->num_check != 0 || row->constr->has_generated_stored
#if !(ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17)
                    || row->constr->has_generated_virtual
#endif
                )))
            {
                return false;
            }

            for (int i = 0; i < row->natts; i++)
            {
                FormData_pg_attribute* column = NativeMethods.TupleDescAttr(row, i);
                if (!column->attisdropped && (column->attidentity != 0 || column->attgenerated != 0 ||
                    NativeMethods.get_typtype(column->atttypid) != (sbyte)'b' || NativeMethods.get_element_type(column->atttypid) != 0))
                {
                    return false;
                }
            }

            return true;
        }
        finally
        {
            NativeMethods.relation_close(table, (int)PgLockMode.None);
        }
    }

    // The check of the rows a COPY gives a partitioned table that has no trigger for its new rows, as the COPY's
    // condition runs it, with the columns as the table's own, or an empty text for any other table. A partition
    // the COPY writes itself takes a trigger for them as the COPY's first row comes.
    private static unsafe string RoutedCheck(uint relid)
    {
        if (NativeMethods.get_rel_relkind(relid) != (sbyte)'p')
        {
            return "";
        }

        RelationData* table = NativeMethods.relation_open(relid, (int)PgLockMode.None);
        try
        {
            Trigger* guard = RoutedGuard(table->trigdesc);
            if (guard is null)
            {
                return "";
            }

            List<(string Name, string Type, List<string> Sequences)> named = CheckedByName(guard, table);
            return named.Count == 0 ? "" : "(" + SequenceRanges.InsertCondition("worf.sequence_range_check", named, "") + ") IS NOT TRUE";
        }
        finally
        {
            NativeMethods.relation_close(table, (int)PgLockMode.None);
        }
    }

    // For each COPY FROM under way, the innermost last, what its first row is to do in the condition worf adds: start
    // the conditions of the triggers that fire after its table's rows, and find the calls its own condition and its
    // columns' domains make. Alone tells of a COPY with no condition of its own, and Takes of one whose rows take
    // what the guard's trigger checks from the columns' defaults.
    private static readonly List<(bool Prepares, bool Finds, bool Alone, bool Takes, List<uint>? Identities, short Verifies)> s_copyFirst = [];

    // The sequences of the identity columns whose draws the COPY about to start has go through the guard.
    private static List<uint>? s_copyIdentities;

    // Where PostgreSQL's CopyFromErrorCallback is, which marks a COPY FROM's state among the error contexts, or -1.
    private static nint s_copyErrorCallback;

    // Where PostgreSQL's function that measures a text is, found once.
    private static nint s_measures;

    // Finds CopyFromErrorCallback in the server, once for the backend, or gives zero where the server doesn't show it.
    private static nint CopyErrorCallback()
    {
        if (s_copyErrorCallback == 0)
        {
            s_copyErrorCallback = NativeLibrary.TryGetExport(NativeLibrary.GetMainProgramHandle(), "CopyFromErrorCallback", out nint address) ? address : -1;
        }

        return s_copyErrorCallback > 0 ? s_copyErrorCallback : 0;
    }

    // The state of the COPY FROM whose row is being worked out, which its error context holds, or null.
    private static unsafe CopyFromStateData* CopyUnderWay()
    {
        for (ErrorContextCallback* context = NativeGlobals.error_context_stack; context is not null; context = context->previous)
        {
            if ((nint)context->callback.DangerousGetAddress() == CopyErrorCallback())
            {
                return (CopyFromStateData*)context->arg;
            }
        }

        return null;
    }

    // Runs with a COPY's first row, in the condition worf adds. The calls of nextval its own condition and its
    // columns' domains make go through the guard from here on, where worf finds them all. The WHEN conditions of the
    // triggers that fire after its table's rows start, as PostgreSQL would start them with the first buffer of rows
    // it writes, so that the guard reaches their calls before the row after. Where no sequence is then left to check
    // for each row, the rows after this one run nothing of worf's: a COPY with no condition of its own drops the
    // condition, and another's works out worf's part of it without a call into worf. The executor state is the one
    // whose memory holds the row's.
    private static unsafe void StartCopyRows()
    {
        (bool prepares, bool finds, bool alone, bool takes, List<uint>? identities, short verify) = s_copyFirst[^1];
        s_copyFirst[^1] = default;
        MemoryContextData* row = NativeGlobals.CurrentMemoryContext;
        EState* state = ExecutorStateOf(row is null ? null : row->parent);
        CopyFromStateData* copy = CopyUnderWay();
        if (state is null || copy is null)
        {
            if (prepares || takes)
            {
                WorfLog.Warning("worf found no executor state for a COPY whose table's rows draw from a managed sequence");
            }

            if (takes)
            {
                GateCopy(s_copyTables[^1]);
            }

            return;
        }

        // A COPY whose rows take the defaults goes on as one that checks each row where worf can't find every call.
        bool found = (finds || takes) && GuardReadCalls(state, copy, takes, identities ?? []);
        if (takes && !found)
        {
            GateCopy(s_copyTables[^1]);
        }

        // The rows of a COPY whose table's triggers may change what a row drew are set against it: the default's
        // expression notes the value where the guard's condition reads it.
        ExprState* drawing = null;
        if (takes && found && verify != 0)
        {
#if ANKUS_PG15
            for (int i = 0; i < copy->num_defaults; i++)
            {
                drawing = copy->defmap[i] == verify - 1 ? copy->defexprs[i] : drawing;
            }
#else
            drawing = copy->defexprs[verify - 1];
#endif
        }

        s_noting = drawing is null ? default : ((nint)drawing, (nint)(&drawing->resvalue), (nint)(&drawing->resnull));
        try
        {
            for (int i = 0; state->es_opened_result_relations is not null && i < state->es_opened_result_relations->length; i++)
            {
                ResultRelInfo* written = (ResultRelInfo*)state->es_opened_result_relations->elements[i].ptr_value;
                if (takes && found && verify == 0)
                {
                    SkipGuard(written);
                }

                AttachGuards(written, takes && found && verify == 0, true, false, state->es_query_cxt);
            }
        }
        finally
        {
            s_noting = default;
        }

        if (takes && found && verify == 0 && Checked(s_copyTables[^1]).Routes)
        {
            s_copyRouting = ((nint)state, s_scopes.Count, s_copying.Count);
            SkipRoutedGuards(state);
        }

        if (prepares)
        {
            StartAfterConditions(state);
        }

        if (s_copying[^1].Count > 0)
        {
            return;
        }

        if (alone)
        {
            copy->whereClause = null;
            return;
        }

        // The call of current_setting, worf's, which comes first, gives way to one that only measures its argument.
        if (s_measures == 0)
        {
            FmgrInfo measures = default;
            NativeMethods.fmgr_info(OctetLengthOid, &measures);
            s_measures = (nint)measures.fn_addr.DangerousGetAddress();
        }

        ExprState* condition = copy->qualexpr;
        for (int i = 0; condition is not null && i < condition->steps_len; i++)
        {
            ExprEvalStep* step = &condition->steps[i];
            if (CallsFunction(StepKind(condition, step)) && step->d.func.finfo->fn_oid == CurrentSettingOid)
            {
                step->d.func.fn_addr = new PGFunction((void*)s_measures);
                return;
            }
        }
    }

    // Has the calls of nextval that a COPY's own condition and the domains its columns hold keep go through the guard,
    // and the COPY check no sequence for each row any more, where they, the calls worf finds in the COPY's defaults
    // and those its tables keep are every call that has started in it so far: one kept where worf doesn't look
    // leaves the COPY checking for each row. A call that starts later, as a column's first value that isn't null
    // starts those of the domain it holds an array of, is found before the row after.
    private static unsafe bool GuardReadCalls(EState* state, CopyFromStateData* copy, bool defaults, List<uint> identities)
    {
        if (s_starting.Count == 0 || s_starting[^1] is not { Copy: true } starting || (starting.CopyState != 0 && starting.CopyState != (nint)state))
        {
            return false;
        }

        ReadNextval();
        Dictionary<nint, nint> states = [];
        s_statesOf = states;
        try
        {
            return GuardReadCalls(state, copy, defaults, identities, starting, states);
        }
        finally
        {
            s_statesOf = null;
        }
    }

    private static unsafe bool GuardReadCalls(EState* state, CopyFromStateData* copy, bool defaults, List<uint> identities, WorfStartingStatement starting,
        Dictionary<nint, nint> states)
    {
        List<(nint Step, bool Single, bool Compiled)> calls = [];
        HashSet<nint> seen = [];
        FindReadCalls(copy, calls, seen);

        // The defaults a COPY works out are those of the columns it doesn't read, and from PostgreSQL 16 on those of
        // the columns it reads too, where a marker may stand for them. Their calls go through the guard too where the
        // COPY holds no gate for them.
        int own = calls.Count;
        for (int i = 0; i < copy->num_defaults; i++)
        {
#if ANKUS_PG15
            FindCalls(copy->defexprs[i], calls, seen, false);
#else
            FindCalls(copy->defexprs[copy->defmap[i]], calls, seen, false);
#endif
        }

#if !ANKUS_PG15
        for (int i = 0; copy->attnumlist is not null && i < copy->attnumlist->length; i++)
        {
            FindCalls(copy->defexprs[copy->attnumlist->elements[i].int_value - 1], calls, seen, false);
        }
#endif

        HashSet<nint> found = [.. calls.Select(call => call.Step), .. RunCalls(state).Select(call => call.Step), .. starting.CopyCalls];
        if (found.Count != starting.Looked || calls.Exists(call => call.Compiled))
        {
            return false;
        }

        if (defaults)
        {
            own = calls.Count;
        }

        ReadHookedCall();
        RegisterEnd();
        HookExit();
        s_snowflake = Worf.CatalogOid("snowflake_node");
        if (!s_planSites.TryGetValue((nint)state, out List<(int Site, nint Call, nint Information)>? sites))
        {
            sites = [];
        }

        // The draws of the identity columns among the defaults run as calls of nextval, as a statement's do.
        List<(nint Step, uint Seqoid, ExprEvalStep Own)> bracketed = [];
        if (identities.Count > 0)
        {
            List<(nint Step, bool Single, bool Compiled)> draws = [];
            s_findingIdentities = true;
            try
            {
                HashSet<nint> looked = [];
                for (int i = 0; i < copy->num_defaults; i++)
                {
#if ANKUS_PG15
                    FindCalls(copy->defexprs[i], draws, looked, false);
#else
                    FindCalls(copy->defexprs[copy->defmap[i]], draws, looked, false);
#endif
                }
            }
            finally
            {
                s_findingIdentities = false;
            }

            HashSet<uint> hooked = [];
            foreach ((nint draw, _, _) in draws)
            {
                ExprEvalStep* step = (ExprEvalStep*)draw;
                uint seqoid = step->d.nextvalueexpr.seqid;
                ExprEvalStep made = *step;
                if (identities.Contains(seqoid) && HookDraw(step, state->es_query_cxt))
                {
                    int site = GuardCall(step);
                    sites.Add((site, (nint)step->d.func.finfo->fn_expr, (nint)step->d.func.fcinfo_data));
                    _ = hooked.Add(seqoid);
                    if (site >= 0)
                    {
                        bracketed.Add((draw, seqoid, made));
                    }
                }
            }

            if (sites.Count > 0)
            {
                s_planSites[(nint)state] = sites;
                starting.CopyState = (nint)state;
            }

            if (!identities.TrueForAll(hooked.Contains))
            {
                return false;
            }
        }

        for (int i = 0; i < own; i++)
        {
            ExprEvalStep* step = (ExprEvalStep*)calls[i].Step;
            int site = GuardCall(step);
            if (site >= 0)
            {
                sites.Add((site, (nint)step->d.func.finfo->fn_expr, (nint)step->d.func.fcinfo_data));
                if (s_sites[site].Item1 != 0)
                {
                    ExprEvalStep direct = *step;
                    direct.d.func.fn_addr = new PGFunction((void*)s_nextval);
                    bracketed.Add((calls[i].Step, s_sites[site].Item1, direct));
                }
            }
        }

        if (sites.Count > 0)
        {
            s_planSites[(nint)state] = sites;
        }

        starting.CopyCalls.UnionWith(found);
        starting.CopyState = (nint)state;
        starting.CopyRead = (nint)copy;
        s_copying[^1].Clear();
        CloseGates(s_copyGates[^1]);
        BracketDraws((nint)state, bracketed, states);
        return true;
    }

    // For each COPY FROM under way, the innermost last, the table it writes.
    private static readonly List<uint> s_copyTables = [];

    // Whether a COPY's rows take, in every column the guard's trigger checks of its table, what the column's own
    // default draws from a managed sequence the trigger checks it under: the COPY reads none of those columns, and
    // each one's default is a call of nextval that names such a sequence, under no more than a cast.
    private static unsafe bool CopyTakesDefaults(uint relid, short[]? columns, List<uint> identities, out short verifies)
    {
        verifies = 0;
        (bool plain, List<(short Column, uint[] Sequences)> guarded, _, bool changed) = Checked(relid);
        if ((!plain && !changed) || guarded.Count == 0 || columns is null || guarded.Exists(column => Array.IndexOf(columns, column.Column) >= 0) ||
            ArguedBy(relid, TriggerTypeInsert).Count > 0)
        {
            return false;
        }

        // A row trigger before the insert may change what a row drew, so the guard's trigger stays, as for a statement.
        if (changed)
        {
            if (guarded.Count != 1 || guarded[0].Sequences.Length != 1 || !ComparedDirectly(relid, guarded[0].Column))
            {
                return false;
            }

            verifies = guarded[0].Column;
        }

        RelationData* table = NativeMethods.relation_open(relid, (int)PgLockMode.AccessShare);
        try
        {
            foreach ((short column, uint[] sequences) in guarded)
            {
                if (!DrawsFrom(Uncast(NativeMethods.build_column_default(table, column)), sequences, identities))
                {
                    return false;
                }
            }
        }
        finally
        {
            NativeMethods.relation_close(table, (int)PgLockMode.None);
        }

        return true;
    }

    // Has a COPY that set out holding no gate check its table's sequences for each row after all, from this row on, as
    // one whose rows the guard's trigger checks does.
    private static void GateCopy(uint relid)
    {
        uint[] drawn = [.. DrawnBy(relid).Where(seqoid => !s_copying[^1].Contains(seqoid))];
        s_copying[^1].AddRange(drawn);
        if (s_copying[^1].Count > 0)
        {
            OpenGates(s_copyGates[^1], s_copying[^1]);
            WaitBeforeDrawing(s_copying[^1]);
        }
    }

    // Finds the calls a COPY's rows make as they are read: those of its condition, and those of the domains the
    // columns it reads hold.
    private static unsafe void FindReadCalls(CopyFromStateData* copy, List<(nint Step, bool Single, bool Compiled)> calls, HashSet<nint> seen)
    {
        FindCalls(copy->qualexpr, calls, seen, false);
        for (int i = 0; copy->attnumlist is not null && i < copy->attnumlist->length; i++)
        {
            FmgrInfo* reads = &copy->in_functions[copy->attnumlist->elements[i].int_value - 1];
            FindReadCalls(reads, reads->fn_mcxt, calls, seen, 0);
        }
    }

    // Finds the calls in the CHECK constraints of the domains whose values a function reads, as PostgreSQL's input and
    // receive functions keep them: a domain's with its constraints, and an array's, a row's, a range's and a
    // multirange's with the functions of what they hold. But for an array's, their states are their own files' to
    // know, and the same from PostgreSQL 15 to 19: a domain's holds four words of what it looked up, the function of
    // its base type, and the reference to its constraints; a row's three words and then, for each column, four words
    // and its function; a range's and a multirange's a pointer and then the function. A function looked up for
    // another keeps that one's memory, which tells a state read right.
    private static unsafe void FindReadCalls(FmgrInfo* reads, MemoryContextData* memory, List<(nint Step, bool Single, bool Compiled)> calls,
        HashSet<nint> seen, int depth)
    {
        if (reads->fn_extra is null || memory is null || reads->fn_mcxt != memory || depth > MaxReadDepth)
        {
            return;
        }

        const int Words = 4 * sizeof(uint);
        byte* state = (byte*)reads->fn_extra;
        switch (reads->fn_oid)
        {
            case DomainInOid or DomainReceiveOid:
                DomainConstraintRef* kept = (DomainConstraintRef*)(state + Words + sizeof(FmgrInfo));
                if (kept->refctx != memory)
                {
                    return;
                }

                for (int i = 0; kept->constraints is not null && i < kept->constraints->length; i++)
                {
                    FindCalls(((DomainConstraintState*)kept->constraints->elements[i].ptr_value)->check_exprstate, calls, seen, false);
                }

                FindReadCalls((FmgrInfo*)(state + Words), memory, calls, seen, depth + 1);
                break;
            case ArrayInOid or ArrayReceiveOid:
                FindReadCalls(&((ArrayMetaState*)state)->proc, memory, calls, seen, depth + 1);
                break;
            case RecordInOid or RecordReceiveOid:
                int columns = *(int*)(state + (2 * sizeof(uint)));
                for (int i = 0; i < columns && columns <= MaxColumns; i++)
                {
                    byte* column = state + Words + (i * (Words + sizeof(FmgrInfo)));
                    FindReadCalls((FmgrInfo*)(column + Words), memory, calls, seen, depth + 1);
                }

                break;
            case RangeInOid or RangeReceiveOid or MultirangeInOid or MultirangeReceiveOid:
                FindReadCalls((FmgrInfo*)(state + sizeof(nint)), memory, calls, seen, depth + 1);
                break;
            default:
                break;
        }
    }

    // Starts the WHEN conditions of the triggers that fire after the rows a COPY's table takes, as TriggerEnabled
    // builds a condition the first time it runs it.
    private static unsafe void StartAfterConditions(EState* state)
    {
        // INNER_VAR and OUTER_VAR, from nodes/primnodes.h, stand for a row's old and new versions in a condition.
        const int Old = 1;
        const int New = 2;
        const int Inner = -1;
        const int Outer = -2;
        const int Row = 1 << 0;
        const int Before = 1 << 1;
        const int Insert = 1 << 2;
        const int Instead = 1 << 6;
        for (int i = 0; state->es_opened_result_relations is not null && i < state->es_opened_result_relations->length; i++)
        {
            ResultRelInfo* written = (ResultRelInfo*)state->es_opened_result_relations->elements[i].ptr_value;
            for (int j = 0; written->ri_TrigDesc is not null && written->ri_TrigWhenExprs is not null && j < written->ri_TrigDesc->numtriggers; j++)
            {
                Trigger* trigger = &written->ri_TrigDesc->triggers[j];
                if (trigger->tgqual is null || written->ri_TrigWhenExprs[j] is not null ||
                    (trigger->tgtype & (Row | Before | Insert | Instead)) != (Row | Insert))
                {
                    continue;
                }

                MemoryContextData* old = Worf.MemoryContextSwitchTo(state->es_query_cxt);
                try
                {
                    Node* condition = (Node*)NativeMethods.stringToNode(trigger->tgqual);
                    NativeMethods.ChangeVarNodes(condition, Old, Inner, 0);
                    NativeMethods.ChangeVarNodes(condition, New, Outer, 0);
                    written->ri_TrigWhenExprs[j] = NativeMethods.ExecPrepareQual(NativeMethods.make_ands_implicit((Expr*)condition), state);
                }
                finally
                {
                    _ = Worf.MemoryContextSwitchTo(old);
                }
            }
        }
    }

    // The expression a SELECT of one column gives, unanalyzed.
    private static unsafe Node* Parsed(string select)
    {
        List* parsed = NativeMethods.raw_parser(Worf.PStrDup(select), RawParseMode.RAW_PARSE_DEFAULT);
        return ((ResTarget*)((SelectStmt*)((RawStmt*)parsed->elements[0].ptr_value)->stmt)->targetList->elements[0].ptr_value)->val;
    }

    // Whether a row inserted into a table, or a partition of it, fires a trigger after it whose WHEN condition may
    // draw from a managed sequence, read once until a table changes.
    // The triggers of the tables read, by table, with the count of table changes they were read at.
    private static readonly Dictionary<uint, List<(uint Table, string Name, uint Function, int Type, char Enabled, bool Internal, string[] Arguments, string? Condition)>> s_treeTriggers = [];
    private static long s_treeTriggersAt = -1;

    // The numbers of pg_trigger's columns tgname, tgfoid, tgtype, tgenabled, tgisinternal, tgargs and tgqual, and of
    // pg_constraint's contype and conbin, found once for the backend.
    private static short[]? s_triggerColumns;
    private static short[]? s_constraintColumns;

    // The tables under a table in its partition tree, read from pg_inherits with no lock taken, as a read of the
    // catalogs takes none: a partition that goes meanwhile has no triggers left to read.
    private static unsafe List<uint> Under(uint relid)
    {
        List<uint> under = [];
        if (NativeMethods.get_rel_relkind(relid) != (sbyte)'p')
        {
            return under;
        }

        List* all = NativeMethods.find_all_inheritors(relid, (int)PgLockMode.None, null);
        for (int i = 0; all is not null && i < all->length; i++)
        {
            if (all->elements[i].oid_value != relid)
            {
                under.Add(all->elements[i].oid_value);
            }
        }

        return under;
    }

    // The triggers of a table and of the tables under it in its partition tree, as pg_trigger holds them, kept
    // until a table changes. They are read straight from the catalog, by its index: a query for them would leave
    // what PostgreSQL looked up to plan it in every session that writes the table.
    private static List<(uint Table, string Name, uint Function, int Type, char Enabled, bool Internal, string[] Arguments, string? Condition)> TreeTriggers(uint relid)
    {
        // pg_trigger, its index by table and name, and its column tgrelid, which every supported version numbers so.
        const uint Triggers = 2620;
        const uint ByTable = 2701;
        const short Table = 2;
        WatchTables();
        if (s_treeTriggersAt != s_tablesStamp)
        {
            s_treeTriggers.Clear();
            s_treeTriggersAt = s_tablesStamp;
        }

        if (s_treeTriggers.TryGetValue(relid, out List<(uint Table, string Name, uint Function, int Type, char Enabled, bool Internal, string[] Arguments, string? Condition)>? found))
        {
            return found;
        }

        unsafe
        {
            s_triggerColumns ??= [.. new[] { "tgname", "tgfoid", "tgtype", "tgenabled", "tgisinternal", "tgargs", "tgqual" }
                .Select(static name => NativeMethods.get_attnum(Triggers, Worf.PStrDup(name)))];
        }

        found = [];
        List<uint> tables = [relid, .. Under(relid)];
        foreach (uint table in tables)
        {
            foreach (object?[] row in Worf.ScanRelation(Triggers, ByTable, [(Table, table)], false, s_triggerColumns))
            {
                // The arguments, each with a zero byte after it.
                List<string> arguments = [];
                byte[] given = (byte[]?)row[5] ?? [];
                for (int start = 0, end; start < given.Length && (end = Array.IndexOf(given, (byte)0, start)) >= 0; start = end + 1)
                {
                    arguments.Add(Worf.FromServer(given.AsSpan(start, end - start)));
                }

                found.Add((table, Worf.FromServer((byte[])row[0]!), (uint)row[1]!, (int)(long)row[2]!, (char)row[3]!, (bool)row[4]!, [.. arguments], (string?)row[6]));
            }
        }

        s_treeTriggers[relid] = found;
        return found;
    }

    // The text of the CHECK constraints of a table, read straight from pg_constraint as TreeTriggers reads triggers.
    private static string CheckText(uint relid)
    {
        // pg_constraint, its index by table, type and name, and its column conrelid, numbered so on every version.
        const uint Constraints = 2606;
        const uint ByTable = 2665;
        short table;
        unsafe
        {
            s_constraintColumns ??= [.. new[] { "contype", "conbin", "conrelid" }.Select(static name => NativeMethods.get_attnum(Constraints, Worf.PStrDup(name)))];
            table = s_constraintColumns[2];
        }

        StringBuilder text = new();
        foreach (string check in Worf.ScanRelation(Constraints, ByTable, [(table, relid)], false, s_constraintColumns.AsSpan(0, 2))
            .Where(static row => (char)row[0]! == 'c').Select(static row => row[1]).OfType<string>())
        {
            _ = text.Append(check).Append(' ');
        }

        return text.ToString();
    }

    // Whether a column's type is a base type that holds nothing else, as no domain, array, row or range is: its
    // values are checked against no domain as they're read or coerced.
    private static bool HoldsNothingElse(uint type)
    {
        unsafe
        {
            return NativeMethods.get_typtype(type) == (sbyte)'b' && NativeMethods.get_element_type(type) == 0;
        }
    }

    // The text of an ordinary table's trigger conditions, as its own entry in PostgreSQL's cache holds them, which
    // the statement that writes the table has loaded, so that no catalog is read for them and the session keeps
    // nothing more: those of the triggers that fire after a row's insert, or those of every trigger, with each
    // trigger's function and the table's CHECK constraints. Null for a table with partitions, whose triggers and
    // constraints count too.
    private static unsafe string? CachedTriggerText(uint relid, bool afterInserts)
    {
        const int Row = 1 << 0;
        const int Before = 1 << 1;
        const int Instead = 1 << 6;
        RelationData* table = NativeMethods.relation_open(relid, (int)PgLockMode.None);
        try
        {
            if (table->rd_rel->relkind != (sbyte)'r')
            {
                return null;
            }

            StringBuilder text = new();
            for (int i = 0; table->trigdesc is not null && i < table->trigdesc->numtriggers; i++)
            {
                Trigger* trigger = &table->trigdesc->triggers[i];
                if (afterInserts && (trigger->tgtype & (Row | Before | TriggerTypeInsert | Instead)) != (Row | TriggerTypeInsert))
                {
                    continue;
                }

                if (trigger->tgqual is not null)
                {
                    _ = text.Append(Worf.FromServer(trigger->tgqual)).Append(' ');
                }

                if (!afterInserts)
                {
                    _ = text.Append(CultureInfo.InvariantCulture, $"{{TRIGGER :funcid {trigger->tgfoid} }} ");
                }
            }

            for (int i = 0; !afterInserts && table->rd_att->constr is not null && i < table->rd_att->constr->num_check; i++)
            {
                _ = text.Append(Worf.FromServer(table->rd_att->constr->check[i].ccbin)).Append(' ');
            }

            return text.ToString();
        }
        finally
        {
            NativeMethods.relation_close(table, (int)PgLockMode.None);
        }
    }

    // The text of the defaults a COPY starts, as CopyDefaultsText reads it, from the table's own entry in
    // PostgreSQL's cache, where every column's type is a base type that holds nothing else: no domain's default or
    // CHECK constraint then counts. Null for any other table.
    private static unsafe string? CachedDefaultsText(uint relid, short[]? columns, bool defaults)
    {
        RelationData* table = NativeMethods.relation_open(relid, (int)PgLockMode.None);
        try
        {
            TupleDescData* row = table->rd_att;
            StringBuilder text = new();
            for (int i = 0; i < row->natts; i++)
            {
                FormData_pg_attribute* column = NativeMethods.TupleDescAttr(row, i);
                if (column->attisdropped || column->attgenerated != 0)
                {
                    continue;
                }

                if (!HoldsNothingElse(column->atttypid))
                {
                    return null;
                }

                if (!(defaults || columns is not null) || (!defaults && Array.IndexOf(columns!, column->attnum) >= 0))
                {
                    continue;
                }

                for (int at = 0; row->constr is not null && at < row->constr->num_defval; at++)
                {
                    if (row->constr->defval[at].adnum == column->attnum && row->constr->defval[at].adbin is not null)
                    {
                        _ = text.Append(Worf.FromServer(row->constr->defval[at].adbin)).Append("  ");
                    }
                }
            }

            return text.ToString();
        }
        finally
        {
            NativeMethods.relation_close(table, (int)PgLockMode.None);
        }
    }

    private static bool AfterConditionsDraw(uint relid)
    {
        WatchTables();
        if (!s_afterConditionsDraw.TryGetValue(relid, out bool draws))
        {
            // A trigger's type tells its row, before, insert and instead bits, from catalog/pg_trigger.h.
            const int Row = 1 << 0;
            const int Before = 1 << 1;
            const int Instead = 1 << 6;
            string conditions = CachedTriggerText(relid, afterInserts: true) ?? string.Join(' ', TreeTriggers(relid)
                .Where(static trigger => trigger.Condition is not null && (trigger.Type & (Row | Before | TriggerTypeInsert | Instead)) == (Row | TriggerTypeInsert))
                .Select(static trigger => trigger.Condition));
            (HashSet<uint> called, _, bool unnamed) = conditions.Length == 0 ? ([], 0, false) : Draws(WithDomainChecks(conditions));
            draws = unnamed || called.Any(s_sequences.ContainsKey);
            s_afterConditionsDraw[relid] = draws;
        }

        return draws;
    }

    /// <summary>
    /// Stops checking the sequences of the innermost COPY FROM that <see cref="CopyCondition"/> started.
    /// </summary>
    internal static void EndCopy()
    {
        if (s_copyRouting.State != 0 && s_copyRouting.Copies == s_copying.Count)
        {
            _ = s_routedMarked.Remove(s_copyRouting.State);
            s_copyRouting = default;
        }

        nint gates = s_copyGates[^1];
        s_copyGates.RemoveAt(s_copyGates.Count - 1);
        s_copying.RemoveAt(s_copying.Count - 1);
        s_copyFirst.RemoveAt(s_copyFirst.Count - 1);
        s_copyTables.RemoveAt(s_copyTables.Count - 1);
        CloseGates(gates);
    }

    /// <summary>
    /// Checks the sequences the innermost COPY FROM draws from, after a row's defaults drew their values.
    /// </summary>
    private static unsafe sbyte* CheckCopied()
    {
        if (s_copyFirst.Count > 0 && (s_copyFirst[^1].Prepares || s_copyFirst[^1].Finds || s_copyFirst[^1].Takes))
        {
            StartCopyRows();
        }

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
    /// Reads a sequence's last value, or null when it hasn't issued one, under the lock nextval takes, whatever the
    /// role's rights on the sequence, which pg_sequence_last_value asks for on PostgreSQL 15 and 16.
    /// </summary>
    private static unsafe long? Position(uint seqoid)
    {
        NativeMethods.LockRelationOid(seqoid, (int)PgLockMode.RowExclusive);
        RelationData* rel = NativeMethods.relation_open(seqoid, (int)PgLockMode.None);
        try
        {
            return PagePosition(rel);
        }
        finally
        {
            NativeMethods.relation_close(rel, (int)PgLockMode.None);
        }
    }

    /// <summary>
    /// Notes a neighbor whose subscription this transaction drops, which the transaction's draws wait for still, as they
    /// would but for the drop, until it commits.
    /// </summary>
    /// <param name="nodeId">The neighbor.</param>
    /// <param name="name">Its name, which its waits give, though the drop deleted the rows that held it.</param>
    internal static void DroppingNeighbor(uint nodeId, string name)
    {
        if (!s_droppedRegistered)
        {
            s_droppedRegistered = true;
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, ForgetDroppedNeighbors);
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, ForgetDroppedNeighbors);
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Prepare, ForgetDroppedNeighbors);
        }

        s_droppedNeighbors[nodeId] = name;
    }

    private static void ForgetDroppedNeighbors()
    {
        s_droppedNeighbors.Clear();
        s_droppedRegistered = false;
    }

    /// <summary>
    /// Reads the blocks this node holds again once the range worker, a function or this backend's own transaction has
    /// changed them.
    /// </summary>
    // The fast draws' counter as the blocks' version was last read.
    private static long s_versionReadAt = -1;

    private static void Reload()
    {
        // The changes told to the draws that hold no gate are read first: a change moves the counter every backend
        // reads before it tells them, so those read here are none the blocks read below lack.
        long changes;
        unsafe
        {
            changes = Volatile.Read(ref Shared()[WorfFastDraws.Changes]);
        }

        // A change moves the version and then that counter, one straight after the other, so while the counter stands
        // where the last read found it, the version does too, and its atomic, which costs far more to read, isn't.
        long localVersion = SequenceRanges.LocalVersion;
        if (changes == s_changesRead && s_version >= 0 && localVersion == s_localVersion && s_versionReadAt == changes)
        {
            return;
        }

        long version = SequenceRanges.Version.Value;
        s_versionReadAt = changes;
        if (version == s_version && localVersion == s_localVersion)
        {
            s_changesRead = changes;
            return;
        }

        // A change to the blocks, as a switch, tells backends before its commit sends its invalidations, so the
        // sequences' bounds are read again too.
        ForgetBounds();
        s_sequences.Clear();
        s_grantors.Clear();
        s_confirmedSequences.Clear();
        s_heardBy.Clear();
        s_complete = SequenceRangeHeard.CompleteSince(SequenceRangeHeard.Started);
        s_drawnBy.Clear();
        s_afterConditionsDraw.Clear();
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
        ProcessBlocks(ReadBlocks(sysid, timeline));

        // The sequences a statement's text names by name are the managed ones of that name, which change with them.
        long signature = s_sequences.Keys.Aggregate(17L, (hash, seqoid) => unchecked((hash * 31) + seqoid)) ^ s_sequences.Count;
        if (signature != s_managedSignature)
        {
            s_managedSignature = signature;
            s_namedByText.Clear();
            s_drawsByText.Clear();
        }

        s_version = version;
        s_localVersion = localVersion;
        s_changesRead = changes;
    }

    // Whether a sequence's values go up, read from PostgreSQL's cache of sequences, or null for a sequence dropped.
    private static unsafe bool? Ascends(uint seqoid)
    {
        // pg_sequence's seqincrement, its fourth column on every version.
        const short Increment = 4;
#if ANKUS_PG19
        const SysCacheIdentifier Sequences = SysCacheIdentifier.SEQRELID;
#else
        const int Sequences = SequenceCacheId;
#endif
        HeapTupleData* tuple = NativeMethods.SearchSysCache1(Sequences, seqoid);
        if (tuple is null)
        {
            return null;
        }

        bool isNull;
        bool ascends = (long)NativeMethods.SysCacheGetAttr(Sequences, tuple, Increment, &isNull) > 0;
        NativeMethods.ReleaseSysCache(tuple);
        return ascends;
    }

    // Reads every managed sequence's blocks as last committed: each sequence with whether its values go up and, for
    // each block of it that is next, active, retired or imported, the block's bounds and state, whether it is this
    // server's own, whether it came from the reserve, the node its draws wait for and the node it came from. The
    // tables are read straight, row by row: a query over them would leave what PostgreSQL looked up to plan it, a
    // few hundred kilobytes, in every session that draws from any sequence.
    private static List<(uint Seqoid, bool Ascending, long? Lo, long Hi, string State, bool Own, bool Reserved, uint? WaitFor, uint? From)> ReadBlocks(
        ulong sysid, uint timeline)
    {
        List<(uint Seqoid, bool Ascending, long? Lo, long Hi, string State, bool Own, bool Reserved, uint? WaitFor, uint? From)> read = [];
        if (Worf.CatalogOid("sequence_range") == 0)
        {
            return read;
        }

        unsafe
        {
            NativeMethods.CommandCounterIncrement();
            NativeMethods.PushActiveSnapshot(NativeMethods.GetLatestSnapshot());
        }

        try
        {
            List<object?[]> local = Worf.ScanCatalog("local_node", "node_id");
            List<object?[]> ranges = local.Count == 0 ? [] : Worf.ScanCatalog("sequence_range", "seqoid", "parent_node");
            if (ranges.Count == 0)
            {
                return read;
            }

            Dictionary<uint, List<object?[]>> blocks = [];
            foreach (object?[] block in Worf.ScanCatalog("sequence_range_block", "seqoid", "kind", "state", "lo", "hi", "from_node", "node_id", "sysid",
                "timeline", "received_at", "reserved").Where(static block => (char)block[1]! is 'n' or 'i' && (string)block[2]! is "next" or "active" or "retired" or "imported"))
            {
                if (!blocks.TryGetValue((uint)block[0]!, out List<object?[]>? own))
                {
                    blocks.Add((uint)block[0]!, own = []);
                }

                own.Add(block);
            }

            WorfBlockWaits? waits = null;
            foreach (object?[] node in local)
            {
                uint self = (uint)node[0]!;
                foreach (object?[] range in ranges)
                {
                    uint seqoid = (uint)range[0]!;
                    if (Ascends(seqoid) is not bool ascending)
                    {
                        continue;
                    }

                    if (!blocks.TryGetValue(seqoid, out List<object?[]>? held))
                    {
                        read.Add((seqoid, ascending, null, 0, "", false, false, null, null));
                        continue;
                    }

                    foreach (object?[] block in held)
                    {
                        string state = (string)block[2]!;
                        bool own = (uint)block[6]! == self && decimal.Parse((string)block[7]!, CultureInfo.InvariantCulture) == sysid &&
                            (long)block[8]! == timeline;

                        // Only an active block of this server's own waits for a node.
                        uint? waitFor = null;
                        if (own && state == "active")
                        {
                            waits ??= new(SequenceRangeHeard.Started, s_complete, [.. s_droppedNeighbors.Keys]);
                            waitFor = waits.WaitFor(self, seqoid, (uint?)range[1], (uint?)block[5], (long)block[9]!);
                        }

                        read.Add((seqoid, ascending, (long)block[3]!, (long)block[4]!, state, own, (bool)block[10]!, waitFor, (uint?)block[5]));
                    }
                }
            }

            return read;
        }
        finally
        {
            unsafe
            {
                NativeMethods.PopActiveSnapshot();
            }
        }
    }

    // Takes in the blocks Reload read, and forgets the waits for blocks that are no longer active.
    private static void ProcessBlocks(List<(uint Seqoid, bool Ascending, long? Lo, long Hi, string State, bool Own, bool Reserved, uint? WaitFor, uint? From)> result)
    {
        foreach ((uint seqoid, bool ascending, long? first, long hi, string state, bool own, bool reserved, uint? waits, uint? from) in result)
        {
            if (!s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence))
            {
                sequence = (ascending, []);
                s_sequences.Add(seqoid, sequence);
            }

            if (first is not long lo)
            {
                continue;
            }

            if (own)
            {
                sequence.Blocks.Add((lo, hi, state));
                if (state == "active" && reserved)
                {
                    _ = s_reservedActive.Add((seqoid, lo));
                }

                if (state == "active" && waits is uint waitFor)
                {
                    s_grantors[seqoid] = waitFor;
                    if (from != waitFor)
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

    }
}
