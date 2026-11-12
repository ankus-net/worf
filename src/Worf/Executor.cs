using System.Globalization;
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
    private static ProcessUtility_hook_type s_nextProcessUtilityHook;
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
        s_nextFmgrHook = NativeGlobals.fmgr_hook;
        NativeGlobals.fmgr_hook = FmgrHookCallback;
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
        Node* check = copying ? SequenceRangeGuard.CopyCondition(copied) : null;
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
            if (copying)
            {
                SequenceRangeGuard.EndCopy();
            }

            if (startingCopy || startingUtility)
            {
                SequenceRangeGuard.StartedExpressions();
            }

            LocalIndex.EndStatement(tracking);
            LocalIndex.EndIsolation(isolated);
            usedTemp = DdlReplication.EndStatement(xactFlags, ddl is not null);
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

            // No need to check for internal deletions.
            if ((dropflags & PerformDeletionInternal) != 0)
            {
                return;
            }

            // Dropping worf itself?
            if (classId == Dependency.ExtensionRelationId)
            {
                uint extensionOid = NativeMethods.get_extension_oid(Worf.PStrDup(Worf.ExtensionName), true);
                if (objectId == extensionOid && objectId != 0)
                {
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
        DdlLock.UnfencePending();
        List<uint> written = DdlLock.WrittenTables(queryDesc, eflags);
        if (written.Count > 0)
        {
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
        }
        finally
        {
            if (starting)
            {
                SequenceRangeGuard.StartedExpressions();
            }
        }
    }

    private static void WorfExecutorFinish(QueryDesc* queryDesc)
    {
        // A data-modifying WITH that the statement didn't read to its end writes the rest of its rows as it finishes.
        bool starting = SequenceRangeGuard.StartingRun(queryDesc);
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
        }
        finally
        {
            if (starting)
            {
                SequenceRangeGuard.StartedExpressions();
            }
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
    /// Notes a function about to run or be inlined, asking for the fmgr hook only as the next hook does.
    /// </summary>
    private static bool WorfNeedsFmgrHook(uint fnOid)
    {
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
