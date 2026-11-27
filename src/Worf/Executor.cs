using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Ankus.Postgres;

[assembly: PgSql("dropped-columns", """
    CREATE TABLE worf.dropped_column (
        relid oid NOT NULL,
        attname name NOT NULL,
        attnum smallint NOT NULL,
        PRIMARY KEY (relid, attname)
    );

    COMMENT ON TABLE worf.dropped_column IS
        'Columns dropped from local tables, which apply ignores in rows from nodes that still have them';
    """)]

namespace Worf;

/// <summary>
/// worf executor related functions.
/// </summary>
internal static unsafe partial class Executor
{
    // PERFORM_DELETION_INTERNAL and PERFORM_DELETION_CONCURRENTLY from catalog/dependency.h.
    private const int PerformDeletionInternal = 0x0001;

    // EXEC_FLAG_EXPLAIN_ONLY, and nextval's OID, F_NEXTVAL.
    private const int ExecFlagExplainOnly = 0x0001;
    private const uint NextvalOid = 1574;
    private const int PerformDeletionConcurrently = 0x0002;

    // SESSION_REPLICATION_ROLE_REPLICA from commands/trigger.h.
    private const int SessionReplicationRoleReplica = 1;

    // The SQLSTATE of the error that rolls back a subtransaction worf's drop took or waited for a lock in, which is worf's
    // own and never leaves the drop.
    private const string LockWaited = "WRF02";

    // RVR_MISSING_OK from catalog/namespace.h.
    private const uint RangeVarMissingOk = 1;

    // ACL_TRUNCATE from nodes/parsenodes.h, whose AclMode is 32 bits wide before PostgreSQL 16.
#if ANKUS_PG15
    private const uint AclTruncate = 1 << 4;
#else
    private const ulong AclTruncate = 1 << 4;
#endif

    private static DropBehavior s_lastDropBehavior = DropBehavior.DROP_RESTRICT;
    private static bool s_droppingWorfObj;
    private static object_access_hook_type s_nextObjectAccessHook;
    // DatabaseRelationId, the oid of pg_database.
    private const uint DatabaseRelationId = 1262;

    private static ProcessUtility_hook_type s_nextProcessUtilityHook;
    private static bool s_singleUserStartNoted;
    private static ExecutorStart_hook_type s_nextExecutorStartHook;
    private static ExecutorRun_hook_type s_nextExecutorRunHook;
    private static ExecutorEnd_hook_type s_nextExecutorFinishHook;
    private static ExecutorEnd_hook_type s_nextExecutorEndHook;
    private static amvalidate_function s_nextNeedsFmgrHook;
    private static fmgr_hook_type s_nextFmgrHook;
    private static bool s_installed;

    /// <summary>
    /// Gets the tables truncated by the current statement.
    /// </summary>
    internal static List<uint> TruncatedTables { get; } = [];

    [PgNativeCallback(nameof(WorfProcessUtility))]
    private static partial ProcessUtility_hook_type ProcessUtilityCallback { get; }

    [PgNativeCallback(nameof(WorfObjectAccess))]
    private static partial object_access_hook_type ObjectAccessCallback { get; }

    [PgNativeCallback(nameof(WorfExecutorStart))]
    private static partial ExecutorStart_hook_type ExecutorStartCallback { get; }

    [PgNativeCallback(nameof(WorfExecutorRun))]
    private static partial ExecutorRun_hook_type ExecutorRunCallback { get; }

    [PgNativeCallback(nameof(WorfExecutorFinish))]
    private static partial ExecutorEnd_hook_type ExecutorFinishCallback { get; }

    [PgNativeCallback(nameof(WorfExecutorEnd))]
    private static partial ExecutorEnd_hook_type ExecutorEndCallback { get; }

    [PgNativeCallback(nameof(CheckTruncatePrivilege))]
    private static partial RangeVarGetRelidCallback TruncatePrivilegeCallback { get; }

    [PgNativeCallback(nameof(WorfNeedsFmgrHook))]
    private static partial amvalidate_function NeedsFmgrHookCallback { get; }

    [PgNativeCallback(nameof(WorfFmgrHook))]
    private static partial fmgr_hook_type FmgrHookCallback { get; }

    /// <summary>
    /// Creates the executor state for one relation, as a query that reads or writes it would.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <param name="forwrite">Whether the query writes the relation.</param>
    /// <returns>The executor state.</returns>
    internal static EState* CreateEstateForRelation(RelationData* rel, bool forwrite)
    {
        // Dummy range table entry needed by executor.
        RangeTblEntry* rte = (RangeTblEntry*)NativeMethods.palloc0((ulong)sizeof(RangeTblEntry));
        rte->type = NodeTag.T_RangeTblEntry;
        rte->rtekind = RTEKind.RTE_RELATION;
        rte->relid = rel->rd_id;
        rte->relkind = rel->rd_rel->relkind;

        // Initialize executor state.
        EState* estate = NativeMethods.CreateExecutorState();

        List* rangeTable = NativeMethods.lappend(null, rte);
#if ANKUS_PG15
        NativeMethods.ExecInitRangeTable(estate, rangeTable);
#else
        List* perminfos = null;
        _ = NativeMethods.addRTEPermissionInfo(&perminfos, rte);
#if ANKUS_PG16 || ANKUS_PG17
        NativeMethods.ExecInitRangeTable(estate, rangeTable, perminfos);
#else
        NativeMethods.ExecInitRangeTable(estate, rangeTable, perminfos, NativeMethods.bms_make_singleton(1));
#endif
#endif

        estate->es_output_cid = NativeMethods.GetCurrentCommandId(forwrite);

        return estate;
    }

    /// <summary>
    /// Prepares the per-tuple expression context of an executor state for tuples of a descriptor.
    /// </summary>
    /// <param name="estate">The executor state.</param>
    /// <param name="tupdesc">The tuples' descriptor.</param>
    /// <returns>The expression context.</returns>
    internal static ExprContext* PreparePerTupleEcontext(EState* estate, TupleDescData* tupdesc)
    {
        ExprContext* econtext = estate->es_per_tuple_exprcontext is not null
            ? estate->es_per_tuple_exprcontext
            : NativeMethods.MakePerTupleExprContext(estate);

        MemoryContextData* oldContext = NativeGlobals.CurrentMemoryContext;
        NativeGlobals.CurrentMemoryContext = estate->es_query_cxt;
        econtext->ecxt_scantuple = NativeMethods.ExecInitExtraTupleSlot(estate, null, NativeGlobals.DangerousAddressOf_TTSOpsHeapTuple());
        NativeGlobals.CurrentMemoryContext = oldContext;

        NativeMethods.ExecSetSlotDescriptor(econtext->ecxt_scantuple, tupdesc);

        return econtext;
    }

    /// <summary>
    /// Compiles a row filter for evaluation.
    /// </summary>
    /// <param name="rowFilter">The row filter.</param>
    /// <returns>The expression state.</returns>
    internal static ExprState* PrepareRowFilter(Node* rowFilter)
    {
        uint exprtype = NativeMethods.exprType(rowFilter);
        Node* expr = NativeMethods.coerce_to_target_type(null,   // no UNKNOWN params here
            rowFilter, exprtype, 16 /* BOOLOID */, -1, CoercionContext.COERCION_ASSIGNMENT,
            CoercionForm.COERCE_IMPLICIT_CAST, -1);

        // This should never happen but just to be sure.
        if (expr is null)
        {
            throw new PgException(PgSqlStates.DatatypeMismatch, "cannot cast the row_filter to boolean",
                hint: "You will need to rewrite the row_filter.");
        }

        expr = (Node*)NativeMethods.expression_planner((Expr*)expr);

        return NativeMethods.ExecInitExpr((Expr*)expr, null);
    }

    /// <summary>
    /// Evaluates an expression, as <c>ExecEvalExpr</c> does.
    /// </summary>
    /// <param name="state">The expression state.</param>
    /// <param name="econtext">The expression context.</param>
    /// <param name="isNull">Whether the result is null.</param>
    /// <returns>The result.</returns>
    internal static ulong ExecEvalExpr(ExprState* state, ExprContext* econtext, out bool isNull)
    {
        bool isnull;
        ulong result = state->evalfunc.Invoke(state, econtext, &isnull);
        isNull = isnull;
        return result;
    }

    /// <summary>
    /// Installs the executor hooks.
    /// </summary>
    internal static void ExecutorInit()
    {
        if (s_installed)
        {
            return;
        }

        s_nextProcessUtilityHook = NativeGlobals.ProcessUtility_hook;
        NativeGlobals.ProcessUtility_hook = ProcessUtilityCallback;

        // Object access hook
        s_nextObjectAccessHook = NativeGlobals.object_access_hook;
        NativeGlobals.object_access_hook = ObjectAccessCallback;

        // Executor start hook, which holds back writes to tables another node changes.
        s_nextExecutorStartHook = NativeGlobals.ExecutorStart_hook;
        NativeGlobals.ExecutorStart_hook = ExecutorStartCallback;

        // Executor run and finish hooks, which tell the sequence range guard a statement routing rows to partitions runs.
        s_nextExecutorRunHook = NativeGlobals.ExecutorRun_hook;
        NativeGlobals.ExecutorRun_hook = ExecutorRunCallback;
        s_nextExecutorFinishHook = NativeGlobals.ExecutorFinish_hook;
        NativeGlobals.ExecutorFinish_hook = ExecutorFinishCallback;

        // Executor end hook, which tells the sequence range guard a statement has ended.
        s_nextExecutorEndHook = NativeGlobals.ExecutorEnd_hook;
        NativeGlobals.ExecutorEnd_hook = ExecutorEndCallback;

        // The fmgr hook check, which PostgreSQL makes before it runs or inlines a function that isn't built in, notes
        // the functions DDL being replicated uses.
        s_nextNeedsFmgrHook = NativeGlobals.needs_fmgr_hook;
        NativeGlobals.needs_fmgr_hook = NeedsFmgrHookCallback;

        // The fmgr hook, which runs as the functions the check asked for it for start, as the trigger functions whose
        // C code the sequence range guard can't see draw.
        // Where no other library has the hook, and under a postmaster, worf's runs within PostgreSQL's own call, so
        // that a call of nextval that goes through it draws without a call into worf's managed side.
        s_nextFmgrHook = NativeGlobals.fmgr_hook;
        CountsDraws = s_nextFmgrHook.IsNull && NativeGlobals.IsPostmasterEnvironment;
        s_hookedTriggerAddress = (nint)HookedTriggerCallback.DangerousGetAddress();
        NativeGlobals.fmgr_hook = CountsDraws
            ? new fmgr_hook_type((delegate* unmanaged[Cdecl]<FmgrHookEventType, FmgrInfo*, ulong*, void>)&WorfDrawHook)
            : FmgrHookCallback;
        s_installed = true;
    }

    private static void WorfStartTruncate() => TruncatedTables.Clear();

    private static void WorfFinishTruncate()
    {
        // If this is not worf node, don't do anything.
        WorfLocalNode? localNode = WorfNode.GetLocalNode(false, true);
        if (localNode is null || TruncatedTables.Count == 0)
        {
            return;
        }

        StringBuilder json = new();
        foreach (uint reloid in TruncatedTables)
        {
            // Format the query.
            string nspname = Worf.FromServer(NativeMethods.get_namespace_name(NativeMethods.get_rel_namespace(reloid)));
            string relname = Worf.FromServer(NativeMethods.get_rel_name(reloid));

            WorfLog.Debug3($"truncating the table {nspname}.{relname}");

            // It's easier to construct json manually than via Jsonb API...
            json.Clear();
            json.Append("{\"schema_name\": ");
            EscapeJson(json, nspname);
            json.Append(",\"table_name\": ");
            EscapeJson(json, relname);
            json.Append('}');

            List<WorfRepSet> repsets = ReplicationSet.GetTableReplicationSets(localNode.Node.Id, reloid);

            if (repsets.Count != 0)
            {
                List<string> repsetNames = [];

                foreach (WorfRepSet repset in repsets)
                {
                    repsetNames.Add(repset.Name);
                    WorfLog.Debug1($"truncating the table {nspname}.{relname} for {repset.Name} repset");
                }

                // Queue the truncate for replication.
                Queue.QueueMessage(repsetNames, NativeMethods.GetUserId(), Queue.QueueCommandTypeTruncate, json.ToString());
            }
        }

        TruncatedTables.Clear();
    }

    private static void WorfProcessUtility(PlannedStmt* pstmt, sbyte* queryString, bool readOnlyTree,
        ProcessUtilityContext context, ParamListInfoData* @params, QueryEnvironment* queryEnv, _DestReceiver* dest,
        QueryCompletion* qc)
    {
        NoteSingleUserStart();
        Node* parsetree = pstmt->utilityStmt;

        // A copy of a table's index on this node alone, which CREATE TABLE ... LIKE builds as a statement of its own, is
        // left out, as the other nodes have no such index to copy.
        if (context == ProcessUtilityContext.PROCESS_UTILITY_SUBCOMMAND && parsetree->type == NodeTag.T_IndexStmt &&
            LocalIndex.SkipsCopy((IndexStmt*)parsetree))
        {
            return;
        }

        s_droppingWorfObj = false;

        // Whether DDL on an index replicates depends on the indexes the statements before built.
        if (context != ProcessUtilityContext.PROCESS_UTILITY_SUBCOMMAND)
        {
            LocalIndex.Flush();
        }

        if (parsetree->type == NodeTag.T_TruncateStmt)
        {
            WorfStartTruncate();
        }

        if (parsetree->type == NodeTag.T_DropStmt)
        {
            s_lastDropBehavior = ((DropStmt*)parsetree)->behavior;
        }

        if (parsetree->type == NodeTag.T_CreatedbStmt)
        {
            Manager.WakeTemplate(TemplateOf((CreatedbStmt*)parsetree));
        }

        // A COPY into or TRUNCATE of a table another node changes waits for the change, where worf is, which alone
        // holds fences. Each takes its table's lock first, as an INSERT does, so that it can't start writing after a
        // fence has reported how far it waited. TRUNCATE checks the privilege before it locks, as PostgreSQL does.
        if (parsetree->type is NodeTag.T_CopyStmt or NodeTag.T_TruncateStmt && DdlLock.MayHoldFences())
        {
            if (parsetree->type == NodeTag.T_CopyStmt && ((CopyStmt*)parsetree)->is_from)
            {
                CheckWrite(((CopyStmt*)parsetree)->relation, PgLockMode.RowExclusive, default);
            }
            else if (parsetree->type == NodeTag.T_TruncateStmt)
            {
                List* relations = ((TruncateStmt*)parsetree)->relations;
                for (int i = 0; relations is not null && i < relations->length; i++)
                {
                    CheckWrite((RangeVar*)relations->elements[i].ptr_value, PgLockMode.AccessExclusive, TruncatePrivilegeCallback);
                }
            }
        }

        // Nor may one write to a table whose indexes on this node alone a statement under way hides.
        LocalIndex.CheckWrite(parsetree);

        SequenceRanges.CheckDdl(parsetree);
        if (parsetree->type != NodeTag.T_TransactionStmt)
        {
            DdlLock.UnfencePending();
        }

        if (SequenceRanges.SkipsDumpedGuard(parsetree))
        {
            return;
        }

        // Which objects a DROP drops is known only before it runs, and whether a statement uses temporary objects only
        // as it runs.
        string? ddl = DdlReplication.CommandToQueue(pstmt, queryString, context, out bool sessionLocal);

        // worf's drop, once DDL replication has had its say on what it drops with, by a role PostgreSQL lets drop it,
        // takes worf's tables at once, which PostgreSQL takes one by one as it finds what the drop drops, so that it
        // doesn't deadlock with worf's workers. With CASCADE it's refused on a provider before it takes anything, and
        // takes the tables whose triggers it drops too, warning of the sequences that keep their blocks' bounds; without,
        // PostgreSQL refuses it while those triggers depend on worf, so it takes none of them ahead of that.
        if (parsetree->type == NodeTag.T_DropStmt && DropsWorf((DropStmt*)parsetree, out bool owner) && owner)
        {
            bool cascade = ((DropStmt*)parsetree)->behavior == DropBehavior.DROP_CASCADE;
            if (cascade)
            {
                RefuseDropWhileProviding();
            }

            LockForWorfDrop(cascade);
            KeepOrWidenBounds();
        }

        (uint Role, string Settings) ddlContext = default;
        if (ddl is not null)
        {
            ddlContext = DdlReplication.Context();
            DdlReplication.WithSnapshot(() =>
            {
                DdlLock.LockNodes(parsetree);
                return true;
            });
        }

        // DDL that replicates can't make other objects use an index on this node alone, and neither it nor DDL from
        // another node chooses names or finds indexes to use by those indexes, which the other nodes don't have.
        Dictionary<uint, (string Name, string?[] Ties)>? ties = ddl is null ? null : LocalIndex.Ties(parsetree);
        bool isolated = LocalIndex.Isolate(parsetree, ddl is not null, context == ProcessUtilityContext.PROCESS_UTILITY_SUBCOMMAND);

        // A COPY FROM draws a buffer of values from a managed sequence before the guard sees the first of them, so it
        // checks the sequences for each row in a condition of its own, ahead of any it has.
        bool copying = parsetree->type == NodeTag.T_CopyStmt && ((CopyStmt*)parsetree)->is_from && ((CopyStmt*)parsetree)->relation is not null;
        uint copied = copying ? NativeMethods.RangeVarGetRelidExtended(((CopyStmt*)parsetree)->relation, 0, RangeVarMissingOk, default, null) : 0;
        string copyCondition = copied != 0 ? SequenceRangeGuard.CopyConditionText(copied, ((CopyStmt*)parsetree)->whereClause) : "";
        Node* check = copying ? SequenceRangeGuard.CopyCondition(copied, copyCondition) : null;
        if (check is not null)
        {
            if (readOnlyTree)
            {
                pstmt = (PlannedStmt*)NativeMethods.copyObjectImpl(pstmt);
                parsetree = pstmt->utilityStmt;
                readOnlyTree = false;
            }

            CopyStmt* copy = (CopyStmt*)parsetree;
            copy->whereClause = copy->whereClause is null ? check
                : (Node*)NativeMethods.makeBoolExpr(BoolExprType.AND_EXPR, NativeMethods.lappend(NativeMethods.lappend(null, check), copy->whereClause), -1);
        }

        // A statement that commits on its way, and then fails, lets its fence go as its transaction aborts.
        // A transaction prepared after it changed worf or a database is noted as it's prepared, and the supervisor wakes
        // the managers once it has ended.
        if (parsetree->type == NodeTag.T_TransactionStmt && ((TransactionStmt*)parsetree)->kind == TransactionStmtKind.TRANS_STMT_PREPARE)
        {
            Worker.NotePrepared();
        }

        // A COPY FROM's calls of nextval, in the defaults it starts before its rows, name the sequences they draw from.
        bool startingCopy = copied != 0;
        short[]? copyRead = null;
        bool copyDefaults = false;
        if (startingCopy)
        {
            CopyStmt* copy = (CopyStmt*)parsetree;
            if (copy->attlist is not null)
            {
                RelationData* rel = NativeMethods.relation_open(copied, (int)PgLockMode.AccessShare);
                List* numbers = NativeMethods.CopyGetAttnums(rel->rd_att, rel, copy->attlist);
                copyRead = new short[numbers is null ? 0 : numbers->length];
                for (int i = 0; i < copyRead.Length; i++)
                {
                    copyRead[i] = (short)numbers->elements[i].int_value;
                }

                NativeMethods.relation_close(rel, (int)PgLockMode.None);
            }

            for (int i = 0; copy->options is not null && i < copy->options->length; i++)
            {
                copyDefaults |= Worf.FromServer(((DefElem*)copy->options->elements[i].ptr_value)->defname) == "default";
            }
        }

        uint xactFlags = DdlReplication.BeginStatement(ddl is not null);
        bool tracking = LocalIndex.BeginStatement(parsetree, ddl is not null);
        if (startingCopy)
        {
            SequenceRangeGuard.StartingCopy(copied, copyRead, copyDefaults, copyCondition);
        }

        // Other utility statements that start expressions outside a plan name the sequences their calls of nextval draw
        // from too.
        bool startingUtility = !startingCopy && SequenceRangeGuard.StartingUtility(parsetree);

        bool usedTemp;
        try
        {
            if (!s_nextProcessUtilityHook.IsNull)
            {
                s_nextProcessUtilityHook.Invoke(pstmt, queryString, readOnlyTree, context, @params, queryEnv, dest, qc);
            }
            else
            {
                NativeMethods.standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, @params, queryEnv, dest, qc);
            }

            // A block that drew outside any plan, as a DO block's assignments do, draws no more.
            if (context == ProcessUtilityContext.PROCESS_UTILITY_TOPLEVEL)
            {
                SequenceRangeGuard.EndCommand();
            }

            // A statement that may change the catalogs, in a subtransaction, ends it with a record decoding takes a
            // base snapshot from.
            if (MayChangeCatalogs(parsetree->type))
            {
                Worf.EndForDecoding();
            }

            // A dropped database's workers leave their slots, which the supervisor frees; a database that worf is
            // created in, or that ALTER DATABASE may have made one to manage, gets its manager once that commits, or,
            // for a prepared transaction, once the supervisor finds it ended.
            if (parsetree->type == NodeTag.T_DropdbStmt)
            {
                Worker.WakeSupervisor();
            }
            else if (parsetree->type == NodeTag.T_CreateExtensionStmt &&
                Worf.FromServer(((CreateExtensionStmt*)parsetree)->extname) == Worf.ExtensionName)
            {
                Worker.ForgetIdleManagersAtCommit(false);
            }
            else if (parsetree->type is NodeTag.T_AlterDatabaseStmt or NodeTag.T_AlterDatabaseSetStmt)
            {
                Worker.ForgetIdleManagersAtCommit(true);
            }
        }
        finally
        {
            // What worf only notes goes first, and the rest goes whatever fails before it: a cancellation stays pending
            // until the statement ends, and fails every call into PostgreSQL meanwhile.
            if (startingCopy || startingUtility)
            {
                SequenceRangeGuard.StartedExpressions();
            }

            try
            {
                if (copying)
                {
                    SequenceRangeGuard.EndCopy();
                }
            }
            finally
            {
                LocalIndex.EndStatement(tracking);
                LocalIndex.EndIsolation(isolated);
                usedTemp = DdlReplication.EndStatement(xactFlags, ddl is not null);
            }
        }

        LocalIndex.Restore(isolated);

        // A DROP leaves out its temporary objects itself, and any other statement that used one stays on this node, or
        // fails when it created or changed objects that outlast the session.
        bool local = sessionLocal || (parsetree->type != NodeTag.T_DropStmt &&
            (ddl is null ? usedTemp : TemporaryObjectUse.UsedBy(parsetree, ddl, usedTemp)));
        if (ddl is not null && !local)
        {
            LocalIndex.CheckTies(ties);
            if (tracking)
            {
                ddl = LocalIndex.NameBuilt(parsetree, ddl);
            }

            DdlReplication.QueueDdl(ddl, ddlContext);
            if (tracking)
            {
                LocalIndex.Queued();
            }
        }

        DdlLock.EndStatement(ddl is not null && !local);

        if (!local)
        {
            DdlReplication.AfterDdl(parsetree);
        }

        SequenceRanges.AfterDdl(parsetree);

        if (parsetree->type == NodeTag.T_TruncateStmt)
        {
            WorfFinishTruncate();
        }
    }

    /// <summary>
    /// Gets the template that CREATE DATABASE copies.
    /// </summary>
    private static string TemplateOf(CreatedbStmt* stmt)
    {
        List* options = stmt->options;
        for (int i = 0; options is not null && i < options->length; i++)
        {
            DefElem* option = (DefElem*)options->elements[i].ptr_value;
            if (Worf.FromServer(option->defname) == "template" && option->arg is not null)
            {
                return Worf.FromServer(NativeMethods.defGetString(option));
            }
        }

        return "template1";
    }

    /// <summary>
    /// Handle object drop. Calls to dependency tracking code.
    /// </summary>
    private static void WorfObjectAccess(ObjectAccessType access, uint classId, uint objectId, int subId, void* arg)
    {
        if (!s_nextObjectAccessHook.IsNull)
        {
            s_nextObjectAccessHook.Invoke(access, classId, objectId, subId, arg);
        }

        TemporaryObjectUse.NoteObjectAccess(access, classId, objectId, subId);

        // A call of nextval is about to run, as its expression starts.
        if (access == ObjectAccessType.OAT_FUNCTION_EXECUTE && objectId == NextvalOid)
        {
            SequenceRangeGuard.BeforeNextval();
        }

        if (access == ObjectAccessType.OAT_POST_CREATE && classId == Dependency.RelationRelationId && subId == 0)
        {
            LocalIndex.NoteCreated(objectId);
        }

        if (access == ObjectAccessType.OAT_DROP)
        {
            // ObjectAccessDrop holds only its dropflags.
            int dropflags = *(int*)arg;
            if (classId == Dependency.RelationRelationId && subId == 0)
            {
                LocalIndex.NoteDropped(objectId, (dropflags & PerformDeletionInternal) != 0, (dropflags & PerformDeletionConcurrently) != 0);
            }

            // A dropped database's sequences give up their change counters once the drop commits, as a dropped sequence
            // gives up its own, so that the sequences of the other databases have counters of their own.
            if (classId == DatabaseRelationId && subId == 0)
            {
                uint dropped = objectId;
                _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, () => SequenceRangeTouches.FreeDatabase(dropped));
            }

            // No need to check for internal deletions.
            if ((dropflags & PerformDeletionInternal) != 0)
            {
                return;
            }

            // Dropping worf itself? A node that provides subscriptions keeps worf: the walsenders that serve them read
            // worf's tables as they decode changes, those made before the drop too, and can't once the drop has removed
            // them, nor serve the subscriptions after it, whose slots would then keep WAL for nothing.
            if (classId == Dependency.ExtensionRelationId)
            {
                uint extensionOid = NativeMethods.get_extension_oid(Worf.PStrDup(Worf.ExtensionName), true);
                if (objectId == extensionOid && objectId != 0)
                {
                    // worf's drop in a database gives up its sequences' change counters once it commits, as the database's
                    // drop does.
                    uint database = NativeGlobals.MyDatabaseId;
                    _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, () => SequenceRangeTouches.FreeDatabase(database));

                    RefuseDropWhileProviding();

                    s_droppingWorfObj = true;
                }
            }

            // Dropping relation within worf?
            if (classId == Dependency.RelationRelationId)
            {
                uint pglnspoid = NativeMethods.get_namespace_oid(Worf.PStrDup(Worf.ExtensionName), true);
                uint relnspoid = NativeMethods.get_rel_namespace(objectId);

                if (pglnspoid == relnspoid)
                {
                    s_droppingWorfObj = true;
                }
            }

            // Don't do extra dependency checks for internal objects, those should be handled by Postgres.
            if (s_droppingWorfObj)
            {
                return;
            }

            // Dropped columns are recorded whether or not this database is a node yet, and a dropped managed sequence
            // leaves range management.
            if (classId == Dependency.RelationRelationId)
            {
                RecordDroppedColumn(objectId, subId);
                if (subId == 0 && NativeMethods.get_rel_relkind(objectId) == (sbyte)'S')
                {
                    Elevated.Run(() => SequenceRanges.NoteDropped(objectId));
                }
            }

            // No local node?
            if (WorfNode.GetLocalNode(false, true) is null)
            {
                return;
            }

            ObjectAddress obj = Dependency.Address(classId, objectId, subId);

            // Under worf.include_ddl_repset, a dropped table leaves every replication set it's in, rather than the
            // drop failing.
            DropBehavior behavior = NativeGlobals.SessionReplicationRole == SessionReplicationRoleReplica ||
                DdlReplication.IncludeDdlRepset
                ? DropBehavior.DROP_CASCADE
                : s_lastDropBehavior;

            Dependency.CheckDependency(obj, behavior);
        }
    }

    // Whether a utility statement may change the catalogs itself, rather than control the transaction or the session,
    // read or copy rows, or run statements of its own, which go through this hook too.
    private static bool MayChangeCatalogs(NodeTag tag) => tag is not (NodeTag.T_TransactionStmt or NodeTag.T_VariableSetStmt or
        NodeTag.T_VariableShowStmt or NodeTag.T_ExplainStmt or NodeTag.T_CopyStmt or NodeTag.T_PrepareStmt or NodeTag.T_ExecuteStmt or
        NodeTag.T_DeallocateStmt or NodeTag.T_FetchStmt or NodeTag.T_ClosePortalStmt or NodeTag.T_DeclareCursorStmt or NodeTag.T_CallStmt or
        NodeTag.T_DoStmt or NodeTag.T_NotifyStmt or NodeTag.T_ListenStmt or NodeTag.T_UnlistenStmt or NodeTag.T_CheckPointStmt or
        NodeTag.T_LoadStmt or NodeTag.T_LockStmt or NodeTag.T_ConstraintsSetStmt);

    private static void WorfExecutorStart(QueryDesc* queryDesc, int eflags)
    {
        if (s_hookFailure is { } failure)
        {
            s_hookFailure = null;
            throw new InvalidOperationException("worf's fmgr hook failed.", failure);
        }

        // worf's own statements are small and gain nothing from JIT, which a low jit_above_cost has PostgreSQL compile
        // them with. One that runs as a session ends, as worf's look at each temporary object PostgreSQL then drops,
        // would stop the server: LLVM leaves a context's release undone in a backend that exits, and PostgreSQL then
        // frees the context again as the subtransaction that owned it ends.
        if (Worf.InOwnSql && queryDesc->plannedstmt is not null)
        {
            queryDesc->plannedstmt->jitFlags = 0;
        }

        NoteSingleUserStart();
        DdlLock.UnfencePending();
        List<uint> written = DdlLock.WrittenTables(queryDesc, eflags);
        if (written.Count > 0)
        {
            // A write to a guarded table holds off worf's drop before it reads worf's tables, which the drop waits for.
            SequenceRangeGuard.HoldOffWorfDrop(written);
            DdlLock.CheckWrite(written);
            LocalIndex.CheckWrites(written);
        }

        SequenceRangeGuard.StartStatement(queryDesc, written);

        // A statement only explained draws nothing as its expressions start, unless it prunes partitions through a call.
        SequenceRangeGuard.StartingExpressions((nint)queryDesc->plannedstmt, (eflags & ExecFlagExplainOnly) != 0, (nint)queryDesc->sourceText,
            queryDesc->@params is not null && queryDesc->@params->numParams > 0);
        try
        {
            if (!s_nextExecutorStartHook.IsNull)
            {
                s_nextExecutorStartHook.Invoke(queryDesc, eflags);
            }
            else
            {
                NativeMethods.standard_ExecutorStart(queryDesc, eflags);
            }
        }
        finally
        {
            SequenceRangeGuard.StartedPlan((nint)queryDesc);
        }
    }

#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
    private static void WorfExecutorRun(QueryDesc* queryDesc, ScanDirection direction, ulong count, bool executeOnce)
#else
    private static void WorfExecutorRun(QueryDesc* queryDesc, ScanDirection direction, ulong count)
#endif
    {
        // A statement starts expressions as it runs, as a VALUES row's, a CHECK's or those of a partition taking a row.
        bool starting = SequenceRangeGuard.StartingRun(queryDesc);

        // Rows that go to the client as they're produced wait on it, as those a query's own consumer takes don't.
        bool toClient = queryDesc->dest is not null && queryDesc->dest->mydest is CommandDest.DestRemote or CommandDest.DestRemoteExecute or
            CommandDest.DestRemoteSimple or CommandDest.DestCopyOut or CommandDest.DestDebug;
        bool noted = SequenceRangeGuard.EnterRun((nint)queryDesc, toClient);
        bool failed = true;
        try
        {
            if (!s_nextExecutorRunHook.IsNull)
            {
#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
                s_nextExecutorRunHook.Invoke(queryDesc, direction, count, executeOnce);
#else
                s_nextExecutorRunHook.Invoke(queryDesc, direction, count);
#endif
            }
            else
            {
#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
                NativeMethods.standard_ExecutorRun(queryDesc, direction, count, executeOnce);
#else
                NativeMethods.standard_ExecutorRun(queryDesc, direction, count);
#endif
            }

            failed = false;
        }
        finally
        {
            if (starting)
            {
                SequenceRangeGuard.StartedExpressions();
            }

            // A SELECT run to its end draws no more, though its portal stays open until the client's next command.
            SequenceRangeGuard.LeaveRun((nint)queryDesc, toClient, noted, failed, count == 0 && queryDesc->operation == CmdType.CMD_SELECT &&
                queryDesc->plannedstmt is not null && !queryDesc->plannedstmt->hasModifyingCTE);
        }
    }

    private static void WorfExecutorFinish(QueryDesc* queryDesc)
    {
        // A data-modifying WITH that the statement didn't read to its end writes the rest of its rows as it finishes.
        bool starting = SequenceRangeGuard.StartingRun(queryDesc);
        bool noted = SequenceRangeGuard.EnterRun((nint)queryDesc, false);
        bool failed = true;
        try
        {
            if (!s_nextExecutorFinishHook.IsNull)
            {
                s_nextExecutorFinishHook.Invoke(queryDesc);
            }
            else
            {
                NativeMethods.standard_ExecutorFinish(queryDesc);
            }

            failed = false;
        }
        finally
        {
            if (starting)
            {
                SequenceRangeGuard.StartedExpressions();
            }

            SequenceRangeGuard.LeaveRun((nint)queryDesc, false, noted, failed, false);
        }
    }

    private static void WorfExecutorEnd(QueryDesc* queryDesc)
    {
        SequenceRangeGuard.EndStatement(queryDesc);

        if (!s_nextExecutorEndHook.IsNull)
        {
            s_nextExecutorEndHook.Invoke(queryDesc);
        }
        else
        {
            NativeMethods.standard_ExecutorEnd(queryDesc);
        }
    }

    /// <summary>
    /// Notes the start of a single-user server, where worf's supervisor doesn't run, as its first statement runs, past
    /// recovery and before the statement writes, so that a restore that starts in single-user mode, writes, and starts
    /// again past the positions its neighbors heard of shows in the starts its messages report.
    /// </summary>
    private static void NoteSingleUserStart()
    {
        if (!s_singleUserStartNoted && !NativeGlobals.IsUnderPostmaster)
        {
            s_singleUserStartNoted = true;
            _ = SequenceRangeHeard.NoteStart();
        }
    }

    /// <summary>
    /// Notes a function about to run or be inlined, asking for the fmgr hook only as the next hook does.
    /// </summary>
    private static bool WorfNeedsFmgrHook(uint fnOid)
    {
        if (SequenceRangeGuard.ReadingHookedCall)
        {
            return true;
        }

        // PostgreSQL asks about a built-in function only as it compiles a call of it with JIT.
        if (fnOid == NextvalOid && SequenceRangeGuard.HooksCompiledCall())
        {
            return true;
        }

        TemporaryObjectUse.NoteFunctionUse(fnOid);
        bool hooked = SequenceRangeGuard.HooksTrigger(fnOid);
        return (!s_nextNeedsFmgrHook.IsNull && s_nextNeedsFmgrHook.Invoke(fnOid)) || hooked;
    }

    /// <summary>
    /// Runs as a function the fmgr hook check asked for the hook for starts, ends or fails, passing it on to the next
    /// hook, which asked for it for its own functions.
    /// </summary>
    private static void WorfFmgrHook(FmgrHookEventType @event, FmgrInfo* flinfo, ulong* @private)
    {
        if (!s_nextFmgrHook.IsNull)
        {
            s_nextFmgrHook.Invoke(@event, flinfo, @private);
        }

        if (@event == FmgrHookEventType.FHET_START)
        {
            SequenceRangeGuard.StartingTrigger(flinfo->fn_oid);
        }
    }

    /// <summary>
    /// Gets whether the fmgr hook counts the draws of the calls of nextval that go through it.
    /// </summary>
    internal static bool CountsDraws { get; private set; }

    // The fmgr hook where no other library has one. PostgreSQL calls it straight, not through Ankus, which costs a
    // call far more than a draw takes, so it may call nothing of PostgreSQL's and let no exception out: an error
    // PostgreSQL raised would leave this frame behind. A call of nextval is the guard's. Any other function the hook
    // check asked for is a trigger function, which runs through HookedTrigger in its place, where worf may call
    // PostgreSQL; a function that goes through the hook for its own settings or owner goes on as it is.
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void WorfDrawHook(FmgrHookEventType @event, FmgrInfo* flinfo, ulong* @private)
    {
        try
        {
            if (flinfo->fn_oid == NextvalOid)
            {
                if (@event == FmgrHookEventType.FHET_START)
                {
                    SequenceRangeGuard.DrawStarting(flinfo, @private);
                }
                else
                {
                    SequenceRangeGuard.DrawEnded(flinfo);
                }
            }
            else if (@event == FmgrHookEventType.FHET_START)
            {
                if (SequenceRangeGuard.WatchesTrigger(flinfo->fn_oid))
                {
                    s_hookedTrigger = ((nint)flinfo, (nint)flinfo->fn_addr.DangerousGetAddress());
                    flinfo->fn_addr = new PGFunction((void*)s_hookedTriggerAddress);
                }
            }
            else if (s_hookedTrigger.Function == (nint)flinfo)
            {
                flinfo->fn_addr = new PGFunction((void*)s_hookedTrigger.Address);
                s_hookedTrigger = default;
            }
        }
        catch (Exception failure) when (failure is NullReferenceException or InvalidOperationException or ArgumentException or
            IndexOutOfRangeException or KeyNotFoundException)
        {
            // Nothing above throws unless worf's own state is wrong. The next statement that starts reports it.
            s_hookFailure ??= failure;
        }
    }

    private static (nint Function, nint Address) s_hookedTrigger;
    private static nint s_hookedTriggerAddress;
    private static Exception? s_hookFailure;

    /// <summary>
    /// Notes a failure of worf's own in code PostgreSQL calls straight, which lets no exception out, for the next
    /// statement that starts to report.
    /// </summary>
    /// <param name="failure">The failure.</param>
    internal static void NoteHookFailure(Exception failure) => s_hookFailure ??= failure;

    [PgNativeCallback(nameof(HookedTrigger))]
    private static partial PGFunction HookedTriggerCallback { get; }

    // Runs a trigger function the fmgr hook watches, in its place, after the guard's check as it starts.
    private static ulong HookedTrigger(FunctionCallInfoBaseData* fcinfo)
    {
        (nint function, nint address) = s_hookedTrigger;
        if (function != (nint)fcinfo->flinfo)
        {
            throw new InvalidOperationException("worf's fmgr hook lost the function it replaced.");
        }

        s_hookedTrigger = default;
        fcinfo->flinfo->fn_addr = new PGFunction((void*)address);
        SequenceRangeGuard.StartingTrigger(fcinfo->flinfo->fn_oid);
        return new PGFunction((void*)address).Invoke(fcinfo);
    }

    // Whether a DROP statement drops worf itself, as DROP EXTENSION worf does, or its schema, which takes worf with it,
    // and whether the role running it owns what it drops of those, as PostgreSQL requires.
    private static bool DropsWorf(DropStmt* drop, out bool owner)
    {
        owner = false;
        if (drop->removeType is not (ObjectType.OBJECT_EXTENSION or ObjectType.OBJECT_SCHEMA) || drop->objects is null)
        {
            return false;
        }

        bool extension = drop->removeType == ObjectType.OBJECT_EXTENSION;
        uint worf = extension ? NativeMethods.get_extension_oid(Worf.PStrDup(Worf.ExtensionName), true)
            : NativeMethods.get_namespace_oid(Worf.PStrDup(Worf.ExtensionName), true);
        for (int i = 0; worf != 0 && i < drop->objects->length; i++)
        {
            RelationData* rel = null;
            if (NativeMethods.get_object_address(drop->removeType, (Node*)drop->objects->elements[i].ptr_value, &rel,
                (int)PgLockMode.AccessShare, true).objectId == worf)
            {
                owner = Worf.ReadScalar<bool>(extension
                    ? "SELECT pg_catalog.pg_has_role(extowner, 'USAGE') FROM pg_catalog.pg_extension WHERE oid = $1"
                    : "SELECT pg_catalog.pg_has_role(nspowner, 'USAGE') FROM pg_catalog.pg_namespace WHERE oid = $1", SpiParameter.Create(worf));
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Gets the key of the lock worf's drop holds for its transaction, 'Worf' as an integer, with its id.
    /// </summary>
    internal const int WorfDropLockKey = 1466921574;

    /// <summary>
    /// Gets the id of the lock worf's drop holds for its transaction.
    /// </summary>
    internal const int WorfDropLockId = 1;

    // Takes the locks worf's drop needs, on worf's tables and, with the guard's triggers, the tables whose triggers it
    // drops, all at once or none: it takes each it can at once, and while one is held, lets those it took go, waits for
    // that one as for a lock, and tries again, so that it never holds some while it waits for another, which a
    // transaction that holds that one may then wait for, whatever order the transaction took its own in.
    private static void LockForWorfDrop(bool triggers)
    {
        // The drop says it's under way first, so that statements waiting for a block, which hold the tables it waits for,
        // stop waiting rather than hold it up.
        SequenceRangeGuard.TakeWorfDropLock();
        List<uint> relids = [.. Elevated.Run(() => Worf.Read("""
            SELECT c.oid FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_class c ON c.oid = d.objid
                JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid AND e.extname = 'worf'
            WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                AND d.deptype = 'e'
            UNION
            SELECT t.tgrelid FROM pg_catalog.pg_trigger t
                JOIN pg_catalog.pg_depend d ON d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass AND d.objid = t.tgfoid
                    AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND d.deptype = 'e'
                JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid AND e.extname = 'worf'
            WHERE $1
            ORDER BY 1
            """, 0, SpiParameter.Create(triggers))).Select(static row => row.Get<uint>(0))];
        while (true)
        {
            uint busy = 0;
            try
            {
                PgTransaction.RunInSubtransaction(() =>
                {
                    foreach (uint relid in relids)
                    {
                        if (!NativeMethods.ConditionalLockRelationOid(relid, (int)PgLockMode.AccessExclusive))
                        {
                            busy = relid;
                            throw new PgException(LockWaited, "a table is held");
                        }
                    }
                });
                return;
            }
            catch (PgException held) when (held.SqlState == LockWaited)
            {
                WaitForTable(busy);
            }
        }
    }

    // Waits for a table worf's drop needs, held by another transaction, as for a lock, and lets it go again. It waits a
    // tenth of a second at a time, so that a worker that queues behind it for the table, as one that holds another table
    // the drop needs may, waits no longer than that, and the drop tries all the tables again.
    private static void WaitForTable(uint relid)
    {
        int nestLevel = NativeMethods.NewGUCNestLevel();
        WorfLocks.SetAtNestLevel("lock_timeout", "100");
        try
        {
            PgTransaction.RunInSubtransaction(() =>
            {
                NativeMethods.LockRelationOid(relid, (int)PgLockMode.AccessExclusive);
                throw new PgException(LockWaited, "the table is free");
            });
        }
        catch (PgException free) when (free.SqlState is LockWaited or LockNotAvailable)
        {
            PgInterrupts.Check();
        }
        finally
        {
            NativeMethods.AtEOXact_GUC(true, nestLevel);
        }
    }

    private const string LockNotAvailable = "55P03";

    // Widens, as worf is dropped, the bounds of each sequence it manages whose values no other node was ever given, out
    // to the end of the values the sequence's range spanned, as past its position they are this node's own; and warns
    // of each other one, which keeps the bounds of the block this node draws from, so that it issues no value another
    // node may hold.
    private static void KeepOrWidenBounds()
    {
        foreach (WorfRow row in Elevated.Run(() => Worf.Read("""
            SELECT r.seqoid::pg_catalog.oid, r.seqoid::pg_catalog.regclass::pg_catalog.text, s.seqmin, s.seqmax, s.seqincrement > 0, r.root_end,
                r.owner_node = l.node_id
                    AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_grant g WHERE g.seqoid = r.seqoid AND g.grantee_id <> l.node_id AND g.kind <> 'x')
                    AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_node n WHERE n.seqoid = r.seqoid AND n.node_id <> l.node_id)
            FROM worf.sequence_range r JOIN pg_catalog.pg_sequence s ON s.seqrelid = r.seqoid CROSS JOIN worf.local_node l
            ORDER BY 2
            """, 0)))
        {
            string name = row.Get<string>(1);
            long min = row.Get<long>(2);
            long max = row.Get<long>(3);
            if (row.Get<bool>(6))
            {
                bool ascending = row.Get<bool>(4);
                long end = row.Get<long>(5);
                _ = Elevated.Run(() => Worf.Execute(
                    ascending ? "UPDATE pg_catalog.pg_sequence SET seqmax = greatest(seqmax, $2) WHERE seqrelid = $1"
                        : "UPDATE pg_catalog.pg_sequence SET seqmin = least(seqmin, $2) WHERE seqrelid = $1",
                    SpiParameter.Create(row.Get<uint>(0)), SpiParameter.Create(end)));
                WorfLog.Log($"sequence {name} goes on to {end}, as no other node was given any of its values");
                continue;
            }

            WorfLog.Warning(new PgDiagnostic($"sequence {name} keeps its bounds, {min} to {max}, those of this node's block")
            {
                Detail = "worf's sequence ranges gave this node's block of the sequence's values to it alone; past it, the sequence would issue values other nodes may hold.",
                Hint = "Once no other node draws from the sequence's values, widen its bounds with ALTER SEQUENCE.",
            });
        }

        unsafe
        {
            NativeMethods.CommandCounterIncrement();
        }
    }

    // Refuses worf's drop on a node that provides subscriptions: the walsenders that serve them read worf's tables as they
    // decode changes, those made before the drop too, and can't once the drop has removed them, nor serve the
    // subscriptions after it, whose slots would then keep WAL for nothing.
    private static void RefuseDropWhileProviding()
    {
        if (!NativeGlobals.IsBinaryUpgrade &&
            Elevated.Run(() => Worf.ReadScalar<string?>("""
                SELECT pg_catalog.string_agg(pg_catalog.quote_ident(slot_name::pg_catalog.text), ', ' ORDER BY slot_name)
                FROM pg_catalog.pg_replication_slots WHERE plugin = 'worf' AND database = pg_catalog.current_database()
                """)) is string slots)
        {
            throw new PgException("55006", "worf can't be dropped while this node provides subscriptions",
                detail: $"Replication slots {slots} stream this database's changes to worf's subscribers.",
                hint: "Drop those subscriptions on their subscribers, which drops their slots, or drop the slots with pg_drop_replication_slot, which gives up the changes their subscribers haven't received.");
        }
    }

    private static void CheckWrite(RangeVar* relation, PgLockMode lockmode, RangeVarGetRelidCallback callback)
    {
        uint relid = relation is null ? 0 : NativeMethods.RangeVarGetRelidExtended(relation, (int)lockmode, RangeVarMissingOk, callback, null);
        if (relid == 0)
        {
            return;
        }

        // TRUNCATE without ONLY empties the tables under the table too, which it locks as PostgreSQL will.
        List<uint> tables = [relid];
        if (lockmode == PgLockMode.AccessExclusive && relation->inh)
        {
            List* inheritors = NativeMethods.find_all_inheritors(relid, (int)lockmode, null);
            for (int i = 0; inheritors is not null && i < inheritors->length; i++)
            {
                tables.Add(inheritors->elements[i].oid_value);
            }
        }

        DdlLock.CheckWrite(tables);
    }

    /// <summary>
    /// Fails a TRUNCATE of a table the user may not truncate before the table is locked, as PostgreSQL does, so that
    /// it doesn't queue for the lock ahead of others.
    /// </summary>
    private static void CheckTruncatePrivilege(RangeVar* relation, uint relId, uint oldRelId, void* arg)
    {
        if (relId != 0 && NativeMethods.pg_class_aclcheck(relId, NativeMethods.GetUserId(), AclTruncate) != AclResult.ACLCHECK_OK)
        {
            NativeMethods.aclcheck_error(AclResult.ACLCHECK_NO_PRIV, NativeMethods.get_relkind_objtype(NativeMethods.get_rel_relkind(relId)),
                relation->relname);
        }
    }

    /// <summary>
    /// Records a column dropped from a table, so that apply ignores it in rows from nodes that drop it later, and
    /// forgets a dropped table's columns.
    /// </summary>
    /// <param name="relid">The table.</param>
    /// <param name="attnum">The column's number, or zero for the whole table.</param>
    private static void RecordDroppedColumn(uint relid, int attnum)
    {
        // Only tables replicate rows, and a temporary table's columns are its session's.
        sbyte relkind = NativeMethods.get_rel_relkind(relid);
        if (relkind is not ((sbyte)'r' or (sbyte)'p') || NativeMethods.get_rel_persistence(relid) == (sbyte)'t' || !HasDroppedColumnTable())
        {
            return;
        }

        if (attnum == 0)
        {
            ForgetDroppedColumns(relid);
            return;
        }

        sbyte* attname = NativeMethods.get_attname(relid, (short)attnum, true);
        if (attnum > 0 && attname is not null)
        {
            string column = Worf.FromServer(attname);
            // worf's catalog is its own, whoever drops the column.
            Elevated.Run(() => _ = Worf.Execute("""
                INSERT INTO worf.dropped_column VALUES ($1, $2::name, $3) ON CONFLICT (relid, attname) DO UPDATE SET attnum = excluded.attnum
                """, SpiParameter.Create(relid), SpiParameter.Create(column), SpiParameter.Create((short)attnum)));
        }
    }

    /// <summary>
    /// Forgets the dropped columns recorded for a table.
    /// </summary>
    private static void ForgetDroppedColumns(uint relid)
        => Elevated.Run(() => _ = Worf.Execute("DELETE FROM worf.dropped_column WHERE relid = $1", SpiParameter.Create(relid)));

    /// <summary>
    /// Checks whether this database has worf's table of dropped columns, as it doesn't before worf is installed.
    /// </summary>
    private static bool HasDroppedColumnTable()
    {
        uint nspoid = NativeMethods.get_namespace_oid(Worf.PStrDup(Worf.ExtensionName), true);
        return nspoid != 0 && NativeMethods.get_relname_relid(Worf.PStrDup("dropped_column"), nspoid) != 0;
    }

    /// <summary>
    /// Appends a string as a JSON string, as <c>escape_json</c> does.
    /// </summary>
    /// <param name="buf">The JSON being built.</param>
    /// <param name="str">The string.</param>
    internal static void EscapeJson(StringBuilder buf, string str)
    {
        buf.Append('"');
        foreach (char c in str)
        {
            switch (c)
            {
                case '\b':
                    buf.Append("\\b");
                    break;
                case '\f':
                    buf.Append("\\f");
                    break;
                case '\n':
                    buf.Append("\\n");
                    break;
                case '\r':
                    buf.Append("\\r");
                    break;
                case '\t':
                    buf.Append("\\t");
                    break;
                case '"':
                    buf.Append("\\\"");
                    break;
                case '\\':
                    buf.Append("\\\\");
                    break;
                default:
                    if (c < ' ')
                    {
                        buf.Append(CultureInfo.InvariantCulture, $"\\u{(int)c:x4}");
                    }
                    else
                    {
                        buf.Append(c);
                    }

                    break;
            }
        }

        buf.Append('"');
    }
}
