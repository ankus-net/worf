using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Ankus.Postgres;

[assembly: PgSql("fences", """
    CREATE TABLE worf.fence (
        fence_id bigint NOT NULL,
        relid oid NOT NULL,
        blocks_writes boolean NOT NULL,
        origin_node oid NOT NULL,
        origin_xid bigint NOT NULL,
        kept_at timestamptz NOT NULL,
        PRIMARY KEY (fence_id, relid)
    );

    COMMENT ON TABLE worf.fence IS
        'Fences that other nodes kept on local tables for DDL that has not applied here yet';
    """)]

namespace Worf;

/// <summary>
/// Locks every node for replicated DDL that can't apply in any order, such as changing a column's type. Before the DDL
/// runs, each node this node subscribes to that receives its DDL holds back its own writes and DDL on the tables and
/// reports how far its WAL has reached. This node, and later each node the DDL reaches, applies every node's changes
/// that far before running the DDL, and a node holds back until the DDL has applied there.
/// </summary>
internal static unsafe partial class DdlLock
{
    // SESSION_REPLICATION_ROLE_REPLICA from commands/trigger.h.
    private const int SessionReplicationRoleReplica = 1;

    // RVR_MISSING_OK from catalog/namespace.h.
    private const uint RangeVarMissingOk = 1;

    // EXEC_FLAG_EXPLAIN_ONLY from executor/executor.h.
    private const int ExplainOnly = 0x0001;

    // The catalogs of relations, namespaces and types, from catalog/pg_*_d.h.
    private const uint RelationRelationId = 1259;
    private const uint NamespaceRelationId = 2615;
    private const uint TypeRelationId = 1247;

    private const string LockNotAvailable = "55P03";

    private const uint LsnOid = 3220;

    // How long a kept fence's DDL may take to arrive before apply asks its origin whether it ever will.
    private static readonly TimeSpan s_reconcileAfter = TimeSpan.FromSeconds(10);

    // The fences this transaction asked other nodes for, with the connections that hold them and the subtransaction
    // that did.
    private static readonly List<(ulong Id, uint NodeId, ulong Lsn, PgReplicationConnection Connection, uint Subxact, List<uint> Relids)> s_heldFences = [];

    // The tables this transaction fenced, which later DDL in it needs no new fence for.
    private static readonly HashSet<uint> s_transactionTables = [];

    // The fences that the DDL this apply transaction ran, or skipped, was for, to let go once it commits.
    private static readonly HashSet<ulong> s_appliedFences = [];

    private static PgSubtransactionCallback? s_subtransactionAbort;
    private static PgSubtransactionCallback? s_subtransactionCommit;

    // Whether this session lets its fences go when it ends, and whether it has read back this database's kept fences.
    private static bool s_releasesOnExit;
    private static bool s_loaded;

    // When apply last asked an origin about kept fences.
    private static long s_reconciledAt;

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
    /// Gets how many tables other nodes' DDL can hold back at once on this server.
    /// </summary>
    [PgGucInt("worf.max_fenced_tables", 4096, "Sets how many tables other nodes' DDL can hold back at once.",
        Minimum = 64, Maximum = 1048576, Context = PgGucContext.Postmaster)]
    internal static partial int MaxFencedTables { get; }

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
        PgSharedMemory.Initialize(Fences, MaxFencedTables, static () => default, static _ => default);
        PgSharedMemory.Initialize(FenceCount, static () => 0);
    }

    /// <summary>
    /// Holds back local writes and DDL on tables while another node changes their definition, and reports how far this
    /// node's WAL has reached once the writes already under way have finished. A node that doesn't receive the other
    /// node's DDL holds nothing back, since nothing would let it go.
    /// </summary>
    /// <returns>The position, or 0/0 when nothing is held back.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.fence_writes(fence_id bigint, origin_node oid, relations text[], timeout_ms integer)
        RETURNS pg_lsn STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        REVOKE ALL ON FUNCTION worf.fence_writes(bigint, oid, text[], integer) FROM public;
        """)]
    [return: PgSqlType("pg_lsn", Schema = "pg_catalog")]
    internal static PgDatum FenceWrites(long fenceId, uint originNode, string?[] relations, int timeoutMs)
    {
        WorfLocalNode? node = WorfNode.GetLocalNode(false, true);
        if (node is null || !WorfNode.GetNodeSubscriptions(node.Node.Id, false)
            .Exists(sub => sub.Enabled && sub.Origin.Id == originNode && sub.ReplicationSets.Contains("ddl_sql")))
        {
            return PgDatum.DangerousCreate(0, LsnOid, PgMemoryContext.Current);
        }

        // Writes to a table this node doesn't replicate never reach the other node, so only its DDL is held back.
        List<(uint Relid, bool BlocksWrites)> fenced = [];
        foreach (string? relation in relations)
        {
            SpiResult result = Worf.SelectCatalog("SELECT to_regclass($1)::oid", SpiParameter.Create(relation));
            if (result.Count > 0 && result[0].Get<uint?>(0) is uint relid)
            {
                fenced.Add((relid, ReplicationSet.GetTableReplicationSets(node.Node.Id, relid).Count > 0));
            }
        }

        // A fence that isn't kept goes with the session that asked for it, as when the other node's DDL fails.
        if (!s_releasesOnExit)
        {
            _ = PgProcess.OnExit(static _ => _ = Release(fence => fence.HolderPid == Environment.ProcessId));
            s_releasesOnExit = true;
        }

        EnsureLoaded();
        AddFences(unchecked((ulong)fenceId), fenced);
        try
        {
            // A ShareLock waits for the writes already under way. Later ones fail on the fence.
            Worker.SetConfigOption("lock_timeout", timeoutMs.ToString(CultureInfo.InvariantCulture), GucContext.PGC_USERSET);
            foreach (uint relid in fenced.Where(fence => fence.BlocksWrites).Select(fence => fence.Relid))
            {
                NativeMethods.LockRelationOid(relid, (int)PgLockMode.Share);
                NativeMethods.UnlockRelationOid(relid, (int)PgLockMode.Share);
            }
        }
        catch
        {
            _ = Release(fence => fence.Id == unchecked((ulong)fenceId) && fence.Dboid == DatabaseId());
            throw;
        }

        // Write and flush a record, so that walsenders move past it and their subscribers report that they have. Its end,
        // unlike the insert position, which skips a page header when the record fills its page, has been written.
        _ = Spi.Execute("SELECT pg_catalog.pg_logical_emit_message(false, 'worf_fence', '')");
        ulong lsn = NativeGlobals.XactLastRecEnd;
        NativeMethods.XLogFlush(lsn);
        return PgDatum.DangerousCreate((nuint)lsn, LsnOid, PgMemoryContext.Current);
    }

    /// <summary>
    /// Keeps a fence after the session that asked for it ends, across restarts, until the change it's for applies
    /// here, or its origin says the transaction that made the change aborted.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.keep_fence(fence_id bigint, origin_node oid, origin_xid bigint)
        RETURNS void STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        REVOKE ALL ON FUNCTION worf.keep_fence(bigint, oid, bigint) FROM public;
        """)]
    internal static void KeepFence(long fenceId, uint originNode, long originXid)
    {
        List<(uint Relid, bool BlocksWrites)> kept = [];
        using (PgLwLockExclusiveGuard<WorfFenceTable, WorfFence> guard = Fences.Exclusive())
        {
            for (int i = 0; i < guard.Length; i++)
            {
                if (guard[i].Id == unchecked((ulong)fenceId) && guard[i].Dboid == DatabaseId())
                {
                    WorfFence fence = guard[i];
                    fence.HolderPid = 0;
                    guard[i] = fence;
                    kept.Add((fence.Relid, fence.BlocksWrites));
                }
            }
        }

        Elevated.Run(() =>
        {
            foreach ((uint relid, bool blocksWrites) in kept)
            {
                _ = Spi.Execute("""
                    INSERT INTO worf.fence VALUES ($1, $2, $3, $4, $5, now()) ON CONFLICT DO NOTHING
                    """, SpiParameter.Create(fenceId), SpiParameter.Create(relid), SpiParameter.Create(blocksWrites),
                    SpiParameter.Create(originNode), SpiParameter.Create(originXid));
            }
        });
    }

    /// <summary>
    /// Lets local writes and DDL through a fence, or through every fence in this database, as when the change it's for
    /// won't reach this node.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.release_fence(fence_id bigint DEFAULT NULL)
        RETURNS integer CALLED ON NULL INPUT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        REVOKE ALL ON FUNCTION worf.release_fence(bigint) FROM public;
        """)]
    internal static int ReleaseFence(long? fenceId)
    {
        _ = Spi.Execute("DELETE FROM worf.fence WHERE $1 IS NULL OR fence_id = $1", SpiParameter.Create(fenceId));
        uint dboid = DatabaseId();
        return Release(fence => fence.Dboid == dboid && (fenceId is null || fence.Id == unchecked((ulong)fenceId.Value)));
    }

    /// <summary>
    /// Fails a local write to a table this node holds back, as an executor's result relations or a COPY's or
    /// TRUNCATE's table, which the caller has locked.
    /// </summary>
    /// <param name="relid">The table.</param>
    internal static void CheckWrite(uint relid) => Check([relid], false);

    /// <summary>
    /// Checks the result relations of a statement about to run, unless it's only explained.
    /// </summary>
    /// <param name="queryDesc">The statement.</param>
    /// <param name="eflags">The executor's flags.</param>
    internal static void CheckWrites(QueryDesc* queryDesc, int eflags)
    {
        PlannedStmt* plan = queryDesc->plannedstmt;
        List<uint> relids = [];
#if ANKUS_PG19
        Bitmapset* results = plan->resultRelationRelids;
        for (int rti = NativeMethods.bms_next_member(results, -1); rti >= 0; rti = NativeMethods.bms_next_member(results, rti))
        {
            relids.Add(((RangeTblEntry*)NativeMethods.list_nth(plan->rtable, rti - 1))->relid);
        }
#else
        List* results = plan->resultRelations;
        for (int i = 0; results is not null && i < results->length; i++)
        {
            relids.Add(((RangeTblEntry*)NativeMethods.list_nth(plan->rtable, results->elements[i].int_value - 1))->relid);
        }
#endif
        if (relids.Count > 0 && (eflags & ExplainOnly) == 0)
        {
            Check(relids, false);
        }
    }

    /// <summary>
    /// Locks every node this node subscribes to for DDL that can't apply in any order, before it runs, and waits until
    /// this node has applied their changes as far as their WAL had reached once they held back their writes. In a
    /// transaction block, any DDL locks the tables it touches, since later DDL there can't wait for other nodes' changes
    /// to tables the transaction has locked.
    /// </summary>
    /// <param name="parsetree">The DDL.</param>
    internal static void LockNodes(Node* parsetree)
    {
        s_statementFence = null;
        if (!GlobalDdlLock)
        {
            return;
        }

        if (parsetree->type == NodeTag.T_IndexStmt && ((IndexStmt*)parsetree)->unique && ((IndexStmt*)parsetree)->concurrent)
        {
            throw new PgException(LockNotAvailable, "worf.global_ddl_lock can't hold for CREATE UNIQUE INDEX CONCURRENTLY, which commits partway",
                hint: "Build the index without CONCURRENTLY, or with worf.global_ddl_lock off.");
        }

        // DDL that someone who may not run it asks for fails by itself, holding back nothing.
        List<uint> relids = LockedTables(parsetree, NativeMethods.IsTransactionBlock());
        _ = relids.RemoveAll(s_transactionTables.Contains);
        if (relids.Count == 0 || !MayRun(parsetree))
        {
            return;
        }

        // Another node's DDL on these tables applies first.
        Check(relids, true);

        // worf reads its catalogs for whoever runs the DDL.
        WorfLocalNode node = WorfNode.GetLocalNode(false, false)!;
        List<(uint NodeId, string Dsn)> peers = Elevated.Run(() => WorfNode.GetNodeSubscriptions(node.Node.Id, false)
            .Where(sub => sub.Enabled)
            .DistinctBy(sub => sub.Origin.Id)
            .Select(sub => (sub.Origin.Id, sub.OriginIf.Dsn))
            .ToList());
        if (peers.Count == 0)
        {
            return;
        }

        ulong fenceId = BitConverter.ToUInt64(RandomNumberGenerator.GetBytes(8)) | 1;
        if (s_heldFences.Count == 0)
        {
            RegisterCallbacks();
        }

        StringBuilder array = new("ARRAY[");
        for (int i = 0; i < relids.Count; i++)
        {
            array.Append(i == 0 ? "" : ", ").Append(Spi.QuoteLiteral(QualifiedName(relids[i])));
        }

        array.Append("]::text[]");

        // A node whose server notices only late that this one died keeps its fence that long, so the fence's session
        // probes the connection.
        List<(uint NodeId, ulong Lsn)> positions = [];
        int timeout = DdlLockTimeout;
        uint subxact = NativeMethods.GetCurrentSubTransactionId();
        foreach ((uint nodeId, string dsn) in peers)
        {
            // The connection holds the fence until the transaction ends, which closes it however this fails.
            int held = s_heldFences.Count;
            s_heldFences.Add((fenceId, nodeId, 0, Worf.Connect(dsn, "worf_ddl_lock", null), subxact, relids));
            PgReplicationConnection conn = s_heldFences[held].Connection;
            string? lsn;
            try
            {
                _ = conn.Query("SELECT pg_catalog.set_config('tcp_keepalives_idle', '10', false), pg_catalog.set_config('tcp_keepalives_interval', '5', false)", 2);
                lsn = conn.Query($"SELECT worf.fence_writes({unchecked((long)fenceId).ToString(CultureInfo.InvariantCulture)}, {node.Node.Id.ToString(CultureInfo.InvariantCulture)}, {array}, {timeout.ToString(CultureInfo.InvariantCulture)})", 1)[0][0];
            }
            catch (PgRemoteException error)
            {
                throw new PgException(LockNotAvailable, $"could not hold back writes on node {nodeId}: {error.Message}");
            }

            // A node that doesn't receive this node's DDL holds nothing back.
            if (!Worf.TryParseLsn(lsn ?? "", out ulong position) || position == 0)
            {
                s_heldFences.RemoveAt(held);
                Close(conn);
                continue;
            }

            s_heldFences[held] = s_heldFences[held] with { Lsn = position };
            positions.Add((nodeId, position));
        }

        WaitForNodes(node.Node.Id, positions, timeout);

        // Another node may have begun DDL on these tables meanwhile, and both give way.
        Check(relids, true);
        s_transactionTables.UnionWith(relids);
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
    /// Waits until this node has applied each node's changes as far as the given WAL position, through every enabled
    /// subscription to it, for the nodes other than itself.
    /// </summary>
    /// <param name="localNodeId">This node.</param>
    /// <param name="positions">The nodes and positions.</param>
    /// <param name="timeoutMs">How long to wait.</param>
    internal static void WaitForNodes(uint localNodeId, List<(uint NodeId, ulong Lsn)> positions, int timeoutMs)
    {
        List<(uint SubId, uint NodeId, ulong Lsn)> waiting = Elevated.Run(() => positions
            .Where(position => position.NodeId != localNodeId)
            .SelectMany(position => WorfNode.GetNodeSubscriptions(position.NodeId, true)
                .Where(sub => sub.Enabled)
                .Select(sub => (sub.Id, position.NodeId, position.Lsn)))
            .ToList());

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
                        if (worker.WorkerType == WorfWorkerType.Apply && worker.Dboid == DatabaseId() &&
                            worker.Worker.Apply.SubId == position.SubId && worker.Worker.Apply.FlushLsn >= position.Lsn)
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

            Worker.WaitLatch(10);
            PgInterrupts.Check();
        }
    }

    /// <summary>
    /// Lets a fence go once the apply transaction that ran or skipped its DDL commits.
    /// </summary>
    /// <param name="fenceId">The fence.</param>
    internal static void ReleaseOnCommit(ulong fenceId)
    {
        if (s_appliedFences.Count == 0)
        {
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.PreCommit, static () => Elevated.Run(() =>
                _ = Spi.Execute("DELETE FROM worf.fence WHERE fence_id = ANY ($1)",
                    SpiParameter.Create(s_appliedFences.Select(id => unchecked((long)id)).ToArray()))));
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, static () =>
            {
                uint dboid = DatabaseId();
                _ = Release(fence => fence.Dboid == dboid && s_appliedFences.Contains(fence.Id));
                s_appliedFences.Clear();
            });
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, static () => s_appliedFences.Clear());
        }

        _ = s_appliedFences.Add(fenceId);
    }

    /// <summary>
    /// Asks a subscription's origin about the fences it kept here whose DDL hasn't arrived after a while: one whose
    /// transaction aborted, or that this node has applied past without letting go, as when its transaction was
    /// skipped, is let go. Apply calls this between transactions.
    /// </summary>
    /// <param name="sub">The subscription.</param>
    internal static void Reconcile(WorfSubscription sub)
    {
        long now = Worker.GetCurrentTimestamp();
        if (now - s_reconciledAt < (long)s_reconcileAfter.TotalMicroseconds)
        {
            return;
        }

        s_reconciledAt = now;
        try
        {
            PgBackgroundWorker.RunTransaction(() =>
            {
                SpiResult kept = Worf.SelectCatalog("""
                    SELECT DISTINCT fence_id, origin_xid FROM worf.fence
                    WHERE origin_node = $1 AND kept_at < now() - $2 * interval '1 microsecond'
                    """, SpiParameter.Create(sub.Origin.Id), SpiParameter.Create((long)s_reconcileAfter.TotalMicroseconds));
                if (kept.Count == 0)
                {
                    return;
                }

                using PgReplicationConnection conn = Worf.Connect(sub.OriginIf.Dsn, sub.Name, "fences");
                for (int i = 0; i < kept.Count; i++)
                {
                    long fenceId = kept[i].Get<long>(0);
                    string?[] row = conn.Query(
                        $"SELECT pg_catalog.pg_xact_status('{kept[i].Get<long>(1).ToString(CultureInfo.InvariantCulture)}'::xid8), pg_catalog.pg_current_wal_insert_lsn()", 2)[0];
                    bool processed = row[0] == "committed" && Worf.TryParseLsn(row[1] ?? "", out ulong position) &&
                        AppliedPast(sub.Origin.Id, position);
                    if (row[0] == "aborted" || processed)
                    {
                        PgLog.Log($"letting go of fence {fenceId} from node {sub.Origin.Name}, whose DDL {(processed ? "this node has gone past" : "never committed")}");
                        _ = Spi.Execute("DELETE FROM worf.fence WHERE fence_id = $1", SpiParameter.Create(fenceId));
                        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, () =>
                            _ = Release(fence => fence.Dboid == DatabaseId() && fence.Id == unchecked((ulong)fenceId)));
                    }
                }
            });
        }
        catch (Exception error) when (error is PgException or PgRemoteException)
        {
            PgLog.Debug1($"could not ask node {sub.Origin.Name} about its fences: {error.Message}");
        }
    }

    /// <summary>
    /// Checks whether every enabled subscription to a node has applied its changes past a WAL position.
    /// </summary>
    private static bool AppliedPast(uint nodeId, ulong lsn)
    {
        List<uint> subs = [.. WorfNode.GetNodeSubscriptions(nodeId, true).Where(sub => sub.Enabled).Select(sub => sub.Id)];
        using PgLwLockShareGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Share();
        return subs.TrueForAll(subId =>
        {
            for (int i = 0; i < guard.Length; i++)
            {
                WorfWorker worker = guard[i];
                if (worker.WorkerType == WorfWorkerType.Apply && worker.Dboid == DatabaseId() && worker.Worker.Apply.SubId == subId &&
                    worker.Worker.Apply.FlushLsn >= lsn)
                {
                    return true;
                }
            }

            return false;
        });
    }

    /// <summary>
    /// Fails a local write, or local DDL, on tables another node holds back, unless apply runs it.
    /// </summary>
    private static void Check(List<uint> relids, bool ddl)
    {
        if (NativeGlobals.SessionReplicationRole == SessionReplicationRoleReplica)
        {
            return;
        }

        EnsureLoaded();
        if (FenceCount.Value == 0)
        {
            return;
        }

        uint dboid = DatabaseId();
        uint held = 0;
        using (PgLwLockShareGuard<WorfFenceTable, WorfFence> guard = Fences.Share())
        {
            for (int i = 0; i < guard.Length && held == 0; i++)
            {
                WorfFence fence = guard[i];
                if (fence.Id != 0 && fence.Dboid == dboid && (ddl || fence.BlocksWrites) && relids.Contains(fence.Relid) &&
                    (fence.HolderPid == 0 || NativeMethods.BackendPidGetProc(fence.HolderPid) is not null))
                {
                    held = fence.Relid;
                }
            }
        }

        if (held != 0)
        {
            throw new PgException(LockNotAvailable,
                $"cannot {(ddl ? "change" : "write to")} table \"{Worf.FromServer(NativeMethods.get_rel_name(held))}\" while another node changes its definition",
                hint: "Retry once the change has applied here, or release it with worf.release_fence.");
        }
    }

    /// <summary>
    /// Reads back this database's kept fences once after the server starts, before any write is checked.
    /// </summary>
    private static void EnsureLoaded()
    {
        if (s_loaded)
        {
            return;
        }

        uint dboid = DatabaseId();
        using (PgLwLockShareGuard<WorfFenceTable, WorfFence> guard = Fences.Share())
        {
            WorfFenceTable table = guard.Header;
            for (int i = 0; i < table.LoadedCount; i++)
            {
                if (table.Loaded[i] == dboid)
                {
                    s_loaded = true;
                    return;
                }
            }
        }

        if (Worf.CatalogOid("fence") == 0)
        {
            return;
        }

        List<(ulong Id, uint Relid, bool BlocksWrites)> kept = [];
        Elevated.Run(() =>
        {
            SpiResult rows = Spi.Select("SELECT fence_id, relid, blocks_writes FROM worf.fence");
            for (int i = 0; i < rows.Count; i++)
            {
                kept.Add((unchecked((ulong)rows[i].Get<long>(0)), rows[i].Get<uint>(1), rows[i].Get<bool>(2)));
            }
        });

        using (PgLwLockExclusiveGuard<WorfFenceTable, WorfFence> guard = Fences.Exclusive())
        {
            WorfFenceTable table = guard.Header;
            for (int i = 0; i < table.LoadedCount; i++)
            {
                if (table.Loaded[i] == dboid)
                {
                    s_loaded = true;
                    return;
                }
            }

            if (table.LoadedCount >= WorfFenceTable.MaxDatabases)
            {
                throw new PgException("53000", $"worf holds fences in at most {WorfFenceTable.MaxDatabases} databases");
            }

            table.Loaded[table.LoadedCount++] = dboid;
            guard.Header = table;
        }

        foreach (IGrouping<ulong, (ulong Id, uint Relid, bool BlocksWrites)> fence in kept.GroupBy(fence => fence.Id))
        {
            AddFences(fence.Key, [.. fence.Select(entry => (entry.Relid, entry.BlocksWrites))], 0);
        }

        s_loaded = true;
    }

    private static int Release(Func<WorfFence, bool> matches)
    {
        int released = 0;
        using (PgLwLockExclusiveGuard<WorfFenceTable, WorfFence> guard = Fences.Exclusive())
        {
            WorfFenceTable table = guard.Header;
            for (int i = 0; i < guard.Length; i++)
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

    private static void AddFences(ulong fenceId, List<(uint Relid, bool BlocksWrites)> relids, int holderPid = -1)
    {
        using PgLwLockExclusiveGuard<WorfFenceTable, WorfFence> guard = Fences.Exclusive();
        WorfFenceTable table = guard.Header;
        int next = 0;
        foreach ((uint relid, bool blocksWrites) in relids)
        {
            int free = -1;
            for (; next < guard.Length && free < 0; next++)
            {
                // A fence whose session ended before its change committed is gone.
                WorfFence fence = guard[next];
                if (fence.Id == 0 ||
                    (fence.HolderPid != 0 && NativeMethods.BackendPidGetProc(fence.HolderPid) is null))
                {
                    if (fence.Id != 0)
                    {
                        table.Used--;
                        _ = PgAtomic.Add(FenceCount, -1);
                    }

                    free = next;
                }
            }

            if (free < 0)
            {
                guard.Header = table;
                throw new PgException(LockNotAvailable, $"too many tables are held back for DDL from other nodes, at most {guard.Length}",
                    hint: "Raise worf.max_fenced_tables, wait for the changes to apply, or release them with worf.release_fence.");
            }

            guard[free] = new WorfFence
            {
                Id = fenceId,
                Dboid = DatabaseId(),
                Relid = relid,
                HolderPid = holderPid < 0 ? Environment.ProcessId : holderPid,
                BlocksWrites = blocksWrites,
            };
            table.Used++;
            _ = PgAtomic.Add(FenceCount, 1);
        }

        guard.Header = table;
    }

    /// <summary>
    /// Lists the tables whose rows from other nodes the DDL could fail to apply if it ran before them, or the reverse:
    /// a table's partitions, a schema's tables, and the tables with columns of a changed domain, enum or composite type.
    /// In a transaction block, it lists the tables of any DDL.
    /// </summary>
    private static List<uint> LockedTables(Node* parsetree, bool inBlock)
    {
        List<uint> tables = [];
        switch (parsetree->type)
        {
            case NodeTag.T_AlterTableStmt when ((AlterTableStmt*)parsetree)->objtype == ObjectType.OBJECT_TYPE:
                uint composite = Relid(((AlterTableStmt*)parsetree)->relation);
                AddTypeUsers(tables, composite == 0 ? 0 : NativeMethods.get_rel_type_id(composite));
                break;

            case NodeTag.T_AlterTableStmt when inBlock || !AppliesInAnyOrder((AlterTableStmt*)parsetree):
                AddTable(tables, Relid(((AlterTableStmt*)parsetree)->relation));
                break;

            case NodeTag.T_RenameStmt when ((RenameStmt*)parsetree)->renameType == ObjectType.OBJECT_SCHEMA:
                AddSchema(tables, Worf.FromServer(((RenameStmt*)parsetree)->subname));
                break;

            case NodeTag.T_RenameStmt:
                AddTable(tables, Relid(((RenameStmt*)parsetree)->relation));
                break;

            case NodeTag.T_AlterObjectSchemaStmt:
                AddTable(tables, Relid(((AlterObjectSchemaStmt*)parsetree)->relation));
                break;

            case NodeTag.T_IndexStmt when inBlock || ((IndexStmt*)parsetree)->unique:
                AddTable(tables, Relid(((IndexStmt*)parsetree)->relation));
                break;

            // Changing a domain's default, or dropping a constraint, leaves its values valid.
#if ANKUS_PG19
            case NodeTag.T_AlterDomainStmt when ((AlterDomainStmt*)parsetree)->subtype is not
                (AlterDomainType.AD_AlterDefault or AlterDomainType.AD_DropNotNull or AlterDomainType.AD_DropConstraint):
#else
            case NodeTag.T_AlterDomainStmt when ((AlterDomainStmt*)parsetree)->subtype is not ((sbyte)'T' or (sbyte)'N' or (sbyte)'X'):
#endif
                AddTypeUsers(tables, TypeOid(((AlterDomainStmt*)parsetree)->typeName));
                break;

            case NodeTag.T_AlterEnumStmt when ((AlterEnumStmt*)parsetree)->oldVal is not null:
                AddTypeUsers(tables, TypeOid(((AlterEnumStmt*)parsetree)->typeName));
                break;

            case NodeTag.T_DropStmt:
                DropStmt* drop = (DropStmt*)parsetree;
                List* objects = drop->objects;
                for (int i = 0; objects is not null && i < objects->length; i++)
                {
                    if (drop->removeType == ObjectType.OBJECT_TABLE)
                    {
                        AddTable(tables, Relid(NativeMethods.makeRangeVarFromNameList((List*)objects->elements[i].ptr_value)));
                    }
                    else if (drop->removeType == ObjectType.OBJECT_SCHEMA && drop->behavior == DropBehavior.DROP_CASCADE)
                    {
                        AddSchema(tables, Worf.FromServer(NativeMethods.NameListToString(NativeMethods.lappend(null, objects->elements[i].ptr_value))));
                    }
                }

                break;

            default:
                break;
        }

        return [.. tables.Distinct()];
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

    /// <summary>
    /// Checks whether the user may run the DDL, by owning its table, schema or type, so that someone who may not
    /// doesn't hold back other nodes.
    /// </summary>
    private static bool MayRun(Node* parsetree)
    {
        uint user = NativeMethods.GetUserId();
        (uint classId, uint objectId) = parsetree->type switch
        {
            NodeTag.T_AlterTableStmt => (RelationRelationId, Relid(((AlterTableStmt*)parsetree)->relation)),
            NodeTag.T_RenameStmt when ((RenameStmt*)parsetree)->renameType == ObjectType.OBJECT_SCHEMA =>
                (NamespaceRelationId, NamespaceOid(Worf.FromServer(((RenameStmt*)parsetree)->subname))),
            NodeTag.T_RenameStmt => (RelationRelationId, Relid(((RenameStmt*)parsetree)->relation)),
            NodeTag.T_AlterObjectSchemaStmt => (RelationRelationId, Relid(((AlterObjectSchemaStmt*)parsetree)->relation)),
            NodeTag.T_IndexStmt => (RelationRelationId, Relid(((IndexStmt*)parsetree)->relation)),
            NodeTag.T_AlterDomainStmt => (TypeRelationId, TypeOid(((AlterDomainStmt*)parsetree)->typeName)),
            NodeTag.T_AlterEnumStmt => (TypeRelationId, TypeOid(((AlterEnumStmt*)parsetree)->typeName)),
            _ => (0u, 0u),
        };

        if (parsetree->type == NodeTag.T_DropStmt)
        {
            DropStmt* drop = (DropStmt*)parsetree;
            List* objects = drop->objects;
            for (int i = 0; objects is not null && i < objects->length; i++)
            {
                uint id = drop->removeType == ObjectType.OBJECT_SCHEMA
                    ? NamespaceOid(Worf.FromServer(NativeMethods.NameListToString(NativeMethods.lappend(null, objects->elements[i].ptr_value))))
                    : Relid(NativeMethods.makeRangeVarFromNameList((List*)objects->elements[i].ptr_value));
                if (id != 0 && !Owns(drop->removeType == ObjectType.OBJECT_SCHEMA ? NamespaceRelationId : RelationRelationId, id, user))
                {
                    return false;
                }
            }

            return true;
        }

        return objectId != 0 && Owns(classId, objectId, user);
    }

    private static bool Owns(uint classId, uint objectId, uint user)
    {
#if ANKUS_PG15
        return classId switch
        {
            RelationRelationId => NativeMethods.pg_class_ownercheck(objectId, user),
            NamespaceRelationId => NativeMethods.pg_namespace_ownercheck(objectId, user),
            _ => NativeMethods.pg_type_ownercheck(objectId, user),
        };
#else
        return NativeMethods.object_ownercheck(classId, objectId, user);
#endif
    }

    private static uint Relid(RangeVar* relation)
        => relation is null ? 0 : NativeMethods.RangeVarGetRelidExtended(relation, 0, RangeVarMissingOk, default, null);

    private static uint NamespaceOid(string name) => NativeMethods.get_namespace_oid(Worf.PStrDup(name), true);

    private static uint TypeOid(List* names)
        => names is null ? 0 : Spi.ExecuteScalar<uint?>("SELECT pg_catalog.to_regtype($1)::oid",
            SpiParameter.Create(Worf.FromServer(NativeMethods.NameListToQuotedString(names)))) ?? 0;

    /// <summary>
    /// Adds a table and, for a partitioned or inherited table, the tables under it.
    /// </summary>
    private static void AddTable(List<uint> tables, uint relid)
    {
        if (relid == 0 || NativeMethods.get_rel_relkind(relid) is not ((sbyte)'r' or (sbyte)'p'))
        {
            return;
        }

        List* inheritors = NativeMethods.find_all_inheritors(relid, 0, null);
        for (int i = 0; inheritors is not null && i < inheritors->length; i++)
        {
            tables.Add(inheritors->elements[i].oid_value);
        }
    }

    private static void AddSchema(List<uint> tables, string schema)
    {
        SpiResult result = Worf.SelectCatalog("""
            SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relkind IN ('r', 'p')
            """, SpiParameter.Create(schema));
        for (int i = 0; i < result.Count; i++)
        {
            tables.Add(result[i].Get<uint>(0));
        }
    }

    private static void AddTypeUsers(List<uint> tables, uint typeOid)
    {
        if (typeOid == 0)
        {
            return;
        }

        SpiResult result = Worf.SelectCatalog("""
            SELECT DISTINCT a.attrelid FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            WHERE a.atttypid IN ($1, (SELECT typarray FROM pg_catalog.pg_type WHERE oid = $1)) AND NOT a.attisdropped
                AND c.relkind IN ('r', 'p')
            """, SpiParameter.Create(typeOid));
        for (int i = 0; i < result.Count; i++)
        {
            tables.Add(result[i].Get<uint>(0));
        }
    }

    private static string QualifiedName(uint relid)
        => Spi.QuoteQualifiedIdentifier(Worf.FromServer(NativeMethods.get_namespace_name(NativeMethods.get_rel_namespace(relid))),
            Worf.FromServer(NativeMethods.get_rel_name(relid)));

    /// <summary>
    /// Keeps the fences when the transaction commits or is prepared, closes their connections once it ends, and in a
    /// subtransaction that rolls back, closes the connections it opened, which lets their fences go.
    /// </summary>
    private static void RegisterCallbacks()
    {
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.PreCommit, KeepHeldFences);
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.PrePrepare, KeepHeldFences);
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, CloseHeldFences);
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Prepare, CloseHeldFences);
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, CloseHeldFences);
        s_subtransactionAbort = PgTransaction.RegisterSubtransactionCallback(PgSubtransactionEvent.Abort, static (subxact, parent) =>
        {
            foreach ((ulong Id, uint NodeId, ulong Lsn, PgReplicationConnection Connection, uint Subxact, List<uint> Relids) fence in
                s_heldFences.Where(fence => fence.Subxact == subxact.Value).ToList())
            {
                Close(fence.Connection);
                _ = s_heldFences.Remove(fence);
                s_transactionTables.ExceptWith(fence.Relids);
                if (s_statementFence?.Id == fence.Id)
                {
                    s_statementFence = null;
                }
            }
        });
        s_subtransactionCommit = PgTransaction.RegisterSubtransactionCallback(PgSubtransactionEvent.Commit, static (subxact, parent) =>
        {
            for (int i = 0; i < s_heldFences.Count; i++)
            {
                if (s_heldFences[i].Subxact == subxact.Value)
                {
                    s_heldFences[i] = s_heldFences[i] with { Subxact = parent.Value };
                }
            }
        });
    }

    private static void KeepHeldFences()
    {
        if (s_heldFences.Count == 0)
        {
            return;
        }

        string nodeId = WorfNode.GetLocalNode(false, false)!.Node.Id.ToString(CultureInfo.InvariantCulture);
        string xid = Spi.ExecuteScalar<long>("SELECT pg_catalog.txid_current()").ToString(CultureInfo.InvariantCulture);
        foreach ((ulong id, _, _, PgReplicationConnection conn, _, _) in s_heldFences)
        {
            _ = conn.Query($"SELECT worf.keep_fence({unchecked((long)id).ToString(CultureInfo.InvariantCulture)}, {nodeId}, {xid})", 1);
        }
    }

    private static void CloseHeldFences()
    {
        // The other nodes drop a fence whose session ends before it's kept.
        foreach ((_, _, _, PgReplicationConnection conn, _, _) in s_heldFences)
        {
            Close(conn);
        }

        s_heldFences.Clear();
        s_transactionTables.Clear();
        s_statementFence = null;
        s_subtransactionAbort?.Dispose();
        s_subtransactionCommit?.Dispose();
        s_subtransactionAbort = null;
        s_subtransactionCommit = null;
    }

    private static void Close(PgReplicationConnection conn)
    {
        try
        {
            conn.Dispose();
        }
        catch (Exception error) when (error is PgException or PgRemoteException)
        {
            // A connection that won't close ends with this process.
        }
    }

    private static uint DatabaseId() => NativeGlobals.MyDatabaseId;
}
