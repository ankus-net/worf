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
    // PERFORM_DELETION_INTERNAL from catalog/dependency.h.
    private const int PerformDeletionInternal = 0x0001;

    // SESSION_REPLICATION_ROLE_REPLICA from commands/trigger.h.
    private const int SessionReplicationRoleReplica = 1;

    private static DropBehavior s_lastDropBehavior = DropBehavior.DROP_RESTRICT;
    private static bool s_droppingWorfObj;
    private static object_access_hook_type s_nextObjectAccessHook;
    private static ProcessUtility_hook_type s_nextProcessUtilityHook;
    private static ExecutorStart_hook_type s_nextExecutorStartHook;
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

            PgLog.Debug3($"truncating the table {nspname}.{relname}");

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
                    PgLog.Debug1($"truncating the table {nspname}.{relname} for {repset.Name} repset");
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

        s_droppingWorfObj = false;

        if (parsetree->type == NodeTag.T_TruncateStmt)
        {
            WorfStartTruncate();
        }

        if (parsetree->type == NodeTag.T_DropStmt)
        {
            s_lastDropBehavior = ((DropStmt*)parsetree)->behavior;
        }

        // A COPY into or TRUNCATE of a table another node changes waits for the change.
        if (parsetree->type == NodeTag.T_CopyStmt && ((CopyStmt*)parsetree)->is_from)
        {
            CheckWrite(((CopyStmt*)parsetree)->relation);
        }
        else if (parsetree->type == NodeTag.T_TruncateStmt)
        {
            List* relations = ((TruncateStmt*)parsetree)->relations;
            for (int i = 0; relations is not null && i < relations->length; i++)
            {
                CheckWrite((RangeVar*)relations->elements[i].ptr_value);
            }
        }

        // Which objects a DROP drops is known only before it runs, and whether a statement uses temporary objects only
        // as it runs.
        string? ddl = DdlReplication.CommandToQueue(pstmt, queryString, context, out bool sessionLocal);
        if (ddl is not null)
        {
            DdlReplication.WithSnapshot(() =>
            {
                DdlLock.LockNodes(parsetree);
                return true;
            });
        }

        uint xactFlags = DdlReplication.BeginStatement(ddl is not null);
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
        }
        finally
        {
            usedTemp = DdlReplication.EndStatement(xactFlags, ddl is not null);
        }

        // A DROP leaves out its temporary objects itself, and any other statement that used one stays on this node.
        bool local = sessionLocal || (usedTemp && parsetree->type != NodeTag.T_DropStmt);
        if (ddl is not null && !local)
        {
            DdlReplication.QueueDdl(ddl);
        }

        if (!local)
        {
            DdlReplication.AfterDdl(parsetree);
        }

        if (parsetree->type == NodeTag.T_TruncateStmt)
        {
            WorfFinishTruncate();
        }
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

        if (access == ObjectAccessType.OAT_DROP)
        {
            // ObjectAccessDrop holds only its dropflags.
            int dropflags = *(int*)arg;

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

            // Dropped columns are recorded whether or not this database is a node yet.
            if (classId == Dependency.RelationRelationId)
            {
                RecordDroppedColumn(objectId, subId);
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

    private static void WorfExecutorStart(QueryDesc* queryDesc, int eflags)
    {
        DdlLock.CheckWrites(queryDesc);

        if (!s_nextExecutorStartHook.IsNull)
        {
            s_nextExecutorStartHook.Invoke(queryDesc, eflags);
        }
        else
        {
            NativeMethods.standard_ExecutorStart(queryDesc, eflags);
        }
    }

    private static void CheckWrite(RangeVar* relation)
    {
        uint relid = relation is null ? 0 : NativeMethods.RangeVarGetRelidExtended(relation, 0, 1, default, null);
        if (relid != 0)
        {
            DdlLock.CheckWrite(relid);
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
            Elevated.Run(() => _ = Spi.Execute("""
                INSERT INTO worf.dropped_column VALUES ($1, $2::name, $3) ON CONFLICT (relid, attname) DO UPDATE SET attnum = excluded.attnum
                """, SpiParameter.Create(relid), SpiParameter.Create(column), SpiParameter.Create((short)attnum)));
        }
    }

    /// <summary>
    /// Forgets the dropped columns recorded for a table.
    /// </summary>
    private static void ForgetDroppedColumns(uint relid)
        => Elevated.Run(() => _ = Spi.Execute("DELETE FROM worf.dropped_column WHERE relid = $1", SpiParameter.Create(relid)));

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
