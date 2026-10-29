using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Locks every node for replicated DDL that can't apply in any order, such as changing a column's type. Before the DDL
/// runs, each node this node subscribes to holds back local writes to its tables and reports how far its WAL has
/// reached. This node, and later each node the DDL reaches, applies every node's changes that far before running the
/// DDL, and a node holds its writes back until the DDL has applied there.
/// </summary>
internal static unsafe partial class DdlLock
{
    private const int MaxFences = 64;

    // SESSION_REPLICATION_ROLE_REPLICA from commands/trigger.h.
    private const int SessionReplicationRoleReplica = 1;

    // RVR_MISSING_OK from catalog/namespace.h.
    private const uint RangeVarMissingOk = 1;

    private const string LockNotAvailable = "55P03";

    private const uint LsnOid = 3220;

    // The fences this transaction asked other nodes for, with the connections that hold them.
    private static readonly List<(ulong Id, uint NodeId, ulong Lsn, PgReplicationConnection Connection)> s_heldFences = [];

    // Whether this session lets its fences go when it ends.
    private static bool s_releasesOnExit;

    // The fence of the DDL statement that runs now, and how far each fenced node's WAL had reached.
    private static (ulong Id, List<(uint NodeId, ulong Lsn)> Positions)? s_statementFence;

    /// <summary>
    /// Gets whether replicated DDL that can't apply in any order locks every node.
    /// </summary>
    [PgGucBool("worf.global_ddl_lock", true, "Lock every node for replicated DDL that can't apply in any order.",
        Context = PgGucContext.SuperuserSet)]
    internal static partial bool GlobalDdlLock { get; }

    /// <summary>
    /// Gets how long DDL waits for other nodes to hold back their writes and catch up.
    /// </summary>
    [PgGucInt("worf.ddl_lock_timeout", 60000, "Sets how long DDL waits for other nodes to hold back writes and catch up.",
        Minimum = 1, Maximum = int.MaxValue, Unit = PgGucUnit.Milliseconds, Context = PgGucContext.SuperuserSet)]
    internal static partial int DdlLockTimeout { get; }

    /// <summary>
    /// Gets the fences this node holds.
    /// </summary>
    internal static PgLwLock<WorfFenceTable, WorfFence> Fences { get; } = new("worf_fences");

    // Whether any fence is held, read without the lock before every write.
    private static PgAtomic<int> FenceCount { get; } = new("worf_fence_count");

    /// <summary>
    /// Sets up the fences' shared memory.
    /// </summary>
    internal static void ShmemInit()
    {
        PgSharedMemory.Initialize(Fences, MaxFences, static () => default, static _ => default);
        PgSharedMemory.Initialize(FenceCount, static () => 0);
    }

    /// <summary>
    /// Holds back local writes to tables while another node changes their definition, and reports how far this node's
    /// WAL has reached once the writes already under way have finished.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.fence_writes(fence_id bigint, relations text[], timeout_ms integer)
        RETURNS pg_lsn STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        REVOKE ALL ON FUNCTION worf.fence_writes(bigint, text[], integer) FROM public;
        """)]
    [return: PgSqlType("pg_lsn", Schema = "pg_catalog")]
    internal static PgDatum FenceWrites(long fenceId, string?[] relations, int timeoutMs)
    {
        List<uint> relids = [];
        foreach (string? relation in relations)
        {
            SpiResult result = Worf.SelectCatalog("SELECT to_regclass($1)::oid", SpiParameter.Create(relation));
            if (result.Count > 0 && result[0].Get<uint?>(0) is uint relid)
            {
                relids.Add(relid);
            }
        }

        // A fence that isn't kept goes with the session that asked for it, as when the other node's DDL fails.
        if (!s_releasesOnExit)
        {
            _ = PgProcess.OnExit(static _ => _ = Release(fence => fence.HolderPid == Environment.ProcessId));
            s_releasesOnExit = true;
        }

        AddFences(unchecked((ulong)fenceId), relids);
        try
        {
            // A ShareLock waits for the writes already under way. Later ones fail on the fence.
            Worker.SetConfigOption("lock_timeout", timeoutMs.ToString(CultureInfo.InvariantCulture), GucContext.PGC_USERSET);
            foreach (uint relid in relids)
            {
                NativeMethods.LockRelationOid(relid, (int)PgLockMode.Share);
                NativeMethods.UnlockRelationOid(relid, (int)PgLockMode.Share);
            }
        }
        catch
        {
            _ = ReleaseFences(unchecked((ulong)fenceId));
            throw;
        }

        // Write and flush a record, so that walsenders move past it and their subscribers report that they have.
        _ = Spi.Execute("SELECT pg_catalog.pg_logical_emit_message(false, 'worf_fence', '')");
        ulong lsn = NativeMethods.GetXLogInsertRecPtr();
        NativeMethods.XLogFlush(lsn);
        return PgDatum.DangerousCreate((nuint)lsn, LsnOid, PgMemoryContext.Current);
    }

    /// <summary>
    /// Keeps a fence after the session that asked for it ends, until the change it's for applies here.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.keep_fence(fence_id bigint)
        RETURNS void STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        REVOKE ALL ON FUNCTION worf.keep_fence(bigint) FROM public;
        """)]
    internal static void KeepFence(long fenceId)
    {
        using PgLwLockExclusiveGuard<WorfFenceTable, WorfFence> guard = Fences.Exclusive();
        for (int i = 0; i < MaxFences; i++)
        {
            if (guard[i].Id == unchecked((ulong)fenceId))
            {
                WorfFence fence = guard[i];
                fence.HolderPid = 0;
                guard[i] = fence;
            }
        }
    }

    /// <summary>
    /// Lets local writes through a fence, or through every fence, as when the change it's for won't reach this node.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.release_fence(fence_id bigint DEFAULT NULL)
        RETURNS integer CALLED ON NULL INPUT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        REVOKE ALL ON FUNCTION worf.release_fence(bigint) FROM public;
        """)]
    internal static int ReleaseFence(long? fenceId) => ReleaseFences(fenceId is long id ? unchecked((ulong)id) : 0);

    /// <summary>
    /// Fails a local write to a table this node holds back, as an executor's result relations or a COPY's or
    /// TRUNCATE's table.
    /// </summary>
    /// <param name="relid">The table.</param>
    internal static void CheckWrite(uint relid)
    {
        if (FenceCount.Value == 0 || NativeGlobals.SessionReplicationRole == SessionReplicationRoleReplica)
        {
            return;
        }

        bool held = false;
        using (PgLwLockShareGuard<WorfFenceTable, WorfFence> guard = Fences.Share())
        {
            for (int i = 0; i < MaxFences && !held; i++)
            {
                WorfFence fence = guard[i];
                held = fence.Id != 0 && fence.Relid == relid && fence.Dboid == NativeGlobals.MyDatabaseId &&
                    (fence.HolderPid == 0 || NativeMethods.BackendPidGetProc(fence.HolderPid) is not null);
            }
        }

        if (held)
        {
            throw new PgException(LockNotAvailable,
                $"cannot write to table \"{Worf.FromServer(NativeMethods.get_rel_name(relid))}\" while another node changes its definition",
                hint: "Retry once the change has applied here, or release it with worf.release_fence.");
        }
    }

    /// <summary>
    /// Checks the result relations of a statement about to run.
    /// </summary>
    /// <param name="queryDesc">The statement.</param>
    internal static void CheckWrites(QueryDesc* queryDesc)
    {
        if (FenceCount.Value == 0)
        {
            return;
        }

        PlannedStmt* plan = queryDesc->plannedstmt;
#if ANKUS_PG19
        Bitmapset* results = plan->resultRelationRelids;
        for (int rti = NativeMethods.bms_next_member(results, -1); rti >= 0; rti = NativeMethods.bms_next_member(results, rti))
        {
            CheckWrite(((RangeTblEntry*)NativeMethods.list_nth(plan->rtable, rti - 1))->relid);
        }
#else
        List* results = plan->resultRelations;
        for (int i = 0; results is not null && i < results->length; i++)
        {
            CheckWrite(((RangeTblEntry*)NativeMethods.list_nth(plan->rtable, results->elements[i].int_value - 1))->relid);
        }
#endif
    }

    /// <summary>
    /// Locks every node this node subscribes to for DDL that can't apply in any order, before it runs, and waits until
    /// this node has applied their changes as far as their WAL had reached once they held back their writes.
    /// </summary>
    /// <param name="parsetree">The DDL.</param>
    internal static void LockNodes(Node* parsetree)
    {
        s_statementFence = null;
        List<string> tables = !GlobalDdlLock ? [] : LockedTables(parsetree);
        if (tables.Count == 0)
        {
            return;
        }

        WorfLocalNode node = WorfNode.GetLocalNode(false, false)!;
        List<(uint NodeId, string Dsn)> peers = [];
        foreach (WorfSubscription sub in WorfNode.GetNodeSubscriptions(node.Node.Id, false))
        {
            if (sub.Enabled && !peers.Exists(peer => peer.NodeId == sub.Origin.Id))
            {
                peers.Add((sub.Origin.Id, sub.OriginIf.Dsn));
            }
        }

        if (peers.Count == 0)
        {
            return;
        }

        ulong fenceId = BitConverter.ToUInt64(RandomNumberGenerator.GetBytes(8)) | 1;
        if (s_heldFences.Count == 0)
        {
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.PreCommit, KeepHeldFences);
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, CloseHeldFences);
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, CloseHeldFences);
        }

        StringBuilder array = new("ARRAY[");
        for (int i = 0; i < tables.Count; i++)
        {
            array.Append(i == 0 ? "" : ", ").Append(Spi.QuoteLiteral(tables[i]));
        }

        array.Append("]::text[]");

        List<(uint NodeId, ulong Lsn)> positions = [];
        int timeout = DdlLockTimeout;
        foreach ((uint nodeId, string dsn) in peers)
        {
            PgReplicationConnection conn = Worf.Connect(dsn, "worf_ddl_lock", null);
            string? lsn;
            try
            {
                lsn = conn.Query($"SELECT worf.fence_writes({unchecked((long)fenceId).ToString(CultureInfo.InvariantCulture)}, {array}, {timeout.ToString(CultureInfo.InvariantCulture)})", 1)[0][0];
            }
            catch (PgRemoteException error)
            {
                conn.Dispose();
                throw new PgException(LockNotAvailable, $"could not hold back writes on node {nodeId}: {error.Message}");
            }

            _ = Worf.TryParseLsn(lsn!, out ulong position);
            s_heldFences.Add((fenceId, nodeId, position, conn));
            positions.Add((nodeId, position));
        }

        WaitForNodes(node.Node.Id, positions, timeout);
        s_statementFence = (fenceId, positions);
    }

    /// <summary>
    /// Takes the fence of the DDL statement that runs now, for its queued message.
    /// </summary>
    /// <returns>The fence and how far each fenced node's WAL had reached, or null.</returns>
    internal static (ulong Id, List<(uint NodeId, ulong Lsn)> Positions)? TakeStatementFence()
    {
        (ulong Id, List<(uint NodeId, ulong Lsn)> Positions)? fence = s_statementFence;
        s_statementFence = null;
        return fence;
    }

    /// <summary>
    /// Waits until this node has applied each node's changes as far as the given WAL position, for the nodes it
    /// subscribes to other than itself.
    /// </summary>
    /// <param name="localNodeId">This node.</param>
    /// <param name="positions">The nodes and positions.</param>
    /// <param name="timeoutMs">How long to wait.</param>
    internal static void WaitForNodes(uint localNodeId, List<(uint NodeId, ulong Lsn)> positions, int timeoutMs)
    {
        List<(uint NodeId, ulong Lsn)> waiting = [];
        foreach ((uint nodeId, ulong lsn) in positions)
        {
            if (nodeId != localNodeId && WorfNode.GetNodeSubscriptions(nodeId, true).Exists(sub => sub.Enabled))
            {
                waiting.Add((nodeId, lsn));
            }
        }

        long deadline = Worker.GetCurrentTimestamp() + (timeoutMs * 1000L);
        while (waiting.Count > 0)
        {
            using (PgLwLockShareGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Share())
            {
                _ = waiting.RemoveAll(position =>
                {
                    for (int i = 0; i < guard.Length; i++)
                    {
                        WorfWorker worker = guard[i];
                        if (worker.WorkerType == WorfWorkerType.Apply && worker.Dboid == NativeGlobals.MyDatabaseId &&
                            worker.Worker.Apply.OriginNodeId == position.NodeId && worker.Worker.Apply.FlushLsn >= position.Lsn)
                        {
                            return true;
                        }
                    }

                    return false;
                });
            }

            if (waiting.Count == 0)
            {
                break;
            }

            if (Worker.GetCurrentTimestamp() > deadline)
            {
                throw new PgException(LockNotAvailable,
                    $"timed out waiting to apply node {waiting[0].NodeId}'s changes up to {Worf.FormatLsn(waiting[0].Lsn)} before DDL");
            }

            NativeMethods.pg_usleep(10000);
            PgInterrupts.Check();
        }
    }

    /// <summary>
    /// Lets local writes through the fences with an id, or through every fence for zero.
    /// </summary>
    /// <param name="fenceId">The fence, or zero.</param>
    /// <returns>The number of tables let through.</returns>
    internal static int ReleaseFences(ulong fenceId) => Release(fence => fenceId == 0 || fence.Id == fenceId);

    private static int Release(Func<WorfFence, bool> matches)
    {
        int released = 0;
        using (PgLwLockExclusiveGuard<WorfFenceTable, WorfFence> guard = Fences.Exclusive())
        {
            WorfFenceTable table = guard.Header;
            for (int i = 0; i < MaxFences; i++)
            {
                if (guard[i].Id != 0 && matches(guard[i]))
                {
                    guard[i] = default;
                    released++;
                }
            }

            table.Used -= released;
            guard.Header = table;
            _ = PgAtomic.Add(FenceCount, -released);
        }

        return released;
    }

    private static void AddFences(ulong fenceId, List<uint> relids)
    {
        using PgLwLockExclusiveGuard<WorfFenceTable, WorfFence> guard = Fences.Exclusive();
        WorfFenceTable table = guard.Header;
        foreach (uint relid in relids)
        {
            int free = -1;
            for (int i = 0; i < MaxFences && free < 0; i++)
            {
                // A fence whose session ended before its change committed is gone.
                WorfFence fence = guard[i];
                if (fence.Id == 0 ||
                    (fence.HolderPid != 0 && NativeMethods.BackendPidGetProc(fence.HolderPid) is null))
                {
                    if (fence.Id != 0)
                    {
                        table.Used--;
                        _ = PgAtomic.Add(FenceCount, -1);
                    }

                    free = i;
                }
            }

            if (free < 0)
            {
                throw new PgException(LockNotAvailable, $"too many tables are held back for DDL from other nodes, at most {MaxFences}",
                    hint: "Wait for the changes to apply, or release them with worf.release_fence.");
            }

            guard[free] = new WorfFence
            {
                Id = fenceId,
                Dboid = NativeGlobals.MyDatabaseId,
                Relid = relid,
                HolderPid = Environment.ProcessId,
            };
            table.Used++;
            _ = PgAtomic.Add(FenceCount, 1);
        }

        guard.Header = table;
    }

    /// <summary>
    /// Lists the tables, by qualified name, whose rows from other nodes the DDL could fail to apply if it ran before
    /// them, or the reverse.
    /// </summary>
    private static List<string> LockedTables(Node* parsetree)
    {
        List<string> tables = [];
        switch (parsetree->type)
        {
            case NodeTag.T_AlterTableStmt when !AppliesInAnyOrder((AlterTableStmt*)parsetree):
                AddTable(tables, ((AlterTableStmt*)parsetree)->relation);
                break;

            case NodeTag.T_RenameStmt:
                AddTable(tables, ((RenameStmt*)parsetree)->relation);
                break;

            case NodeTag.T_AlterObjectSchemaStmt:
                AddTable(tables, ((AlterObjectSchemaStmt*)parsetree)->relation);
                break;

            case NodeTag.T_IndexStmt when ((IndexStmt*)parsetree)->unique:
                AddTable(tables, ((IndexStmt*)parsetree)->relation);
                break;

            case NodeTag.T_DropStmt when ((DropStmt*)parsetree)->removeType == ObjectType.OBJECT_TABLE:
                List* objects = ((DropStmt*)parsetree)->objects;
                for (int i = 0; objects is not null && i < objects->length; i++)
                {
                    AddTable(tables, NativeMethods.makeRangeVarFromNameList((List*)objects->elements[i].ptr_value));
                }

                break;

            default:
                break;
        }

        return tables;
    }

    /// <summary>
    /// Checks whether every part of an ALTER TABLE leaves rows from other nodes that don't have it yet applying as
    /// they did, as adding a column that can be missing, dropping a column or a constraint, or changing settings do.
    /// </summary>
    private static bool AppliesInAnyOrder(AlterTableStmt* stmt)
    {
        List* cmds = stmt->cmds;
        for (int i = 0; cmds is not null && i < cmds->length; i++)
        {
            AlterTableCmd* cmd = (AlterTableCmd*)cmds->elements[i].ptr_value;
            bool inAnyOrder = cmd->subtype switch
            {
                AlterTableType.AT_AddColumn => ColumnCanBeMissing((ColumnDef*)cmd->def),
                AlterTableType.AT_DropColumn or AlterTableType.AT_ColumnDefault or AlterTableType.AT_DropNotNull or
                    AlterTableType.AT_DropConstraint or AlterTableType.AT_SetStatistics or AlterTableType.AT_SetOptions or
                    AlterTableType.AT_ResetOptions or AlterTableType.AT_SetStorage or AlterTableType.AT_SetCompression or
                    AlterTableType.AT_ChangeOwner or AlterTableType.AT_ClusterOn or AlterTableType.AT_DropCluster or
                    AlterTableType.AT_SetRelOptions or AlterTableType.AT_ResetRelOptions or
                    AlterTableType.AT_ReplaceRelOptions or AlterTableType.AT_SetTableSpace or
                    AlterTableType.AT_EnableTrig or AlterTableType.AT_EnableAlwaysTrig or AlterTableType.AT_EnableReplicaTrig or
                    AlterTableType.AT_DisableTrig or AlterTableType.AT_EnableTrigAll or AlterTableType.AT_DisableTrigAll or
                    AlterTableType.AT_EnableTrigUser or AlterTableType.AT_DisableTrigUser or
                    AlterTableType.AT_EnableRowSecurity or AlterTableType.AT_DisableRowSecurity or
                    AlterTableType.AT_ForceRowSecurity or AlterTableType.AT_NoForceRowSecurity or
                    AlterTableType.AT_GenericOptions => true,
                _ => false,
            };
            if (!inAnyOrder)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Checks whether a new column can be missing from rows that other nodes wrote without it: it has no constraint
    /// but NOT NULL with a default.
    /// </summary>
    private static bool ColumnCanBeMissing(ColumnDef* column)
    {
        bool hasDefault = column->raw_default is not null;
        bool notNull = column->is_not_null;
        List* constraints = column->constraints;
        for (int i = 0; constraints is not null && i < constraints->length; i++)
        {
            Constraint* constraint = (Constraint*)constraints->elements[i].ptr_value;
            switch (constraint->contype)
            {
                case ConstrType.CONSTR_DEFAULT:
                    hasDefault = true;
                    break;

                case ConstrType.CONSTR_NULL:
                    break;

                case ConstrType.CONSTR_NOTNULL:
                    notNull = true;
                    break;

                default:
                    return false;
            }
        }

        return !notNull || hasDefault;
    }

    private static void AddTable(List<string> tables, RangeVar* relation)
    {
        uint reloid = relation is null ? 0 : NativeMethods.RangeVarGetRelidExtended(relation, 0, RangeVarMissingOk, default, null);
        if (reloid != 0 && NativeMethods.get_rel_relkind(reloid) == (sbyte)'r')
        {
            tables.Add(Spi.QuoteQualifiedIdentifier(Worf.FromServer(NativeMethods.get_namespace_name(NativeMethods.get_rel_namespace(reloid))),
                Worf.FromServer(NativeMethods.get_rel_name(reloid))));
        }
    }

    private static void KeepHeldFences()
    {
        foreach ((ulong id, _, _, PgReplicationConnection conn) in s_heldFences)
        {
            _ = conn.Query($"SELECT worf.keep_fence({unchecked((long)id).ToString(CultureInfo.InvariantCulture)})", 1);
        }
    }

    private static void CloseHeldFences()
    {
        // The other nodes drop a fence whose session ends before it's kept.
        foreach ((_, _, _, PgReplicationConnection conn) in s_heldFences)
        {
            try
            {
                conn.Dispose();
            }
            catch (Exception)
            {
                // A connection that won't close ends with this process.
            }
        }

        s_heldFences.Clear();
        s_statementFence = null;
    }
}
