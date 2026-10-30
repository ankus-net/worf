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

    // ACL_REFERENCES and ACL_TRIGGER from nodes/parsenodes.h, whose AclMode is 32 bits wide before PostgreSQL 16.
#if ANKUS_PG15
    private const uint AclReferences = 1 << 5;
    private const uint AclTrigger = 1 << 6;
#else
    private const ulong AclReferences = 1 << 5;
    private const ulong AclTrigger = 1 << 6;
#endif

    // The catalogs of relations, namespaces and types, from catalog/pg_*_d.h.
    private const uint RelationRelationId = 1259;
    private const uint NamespaceRelationId = 2615;
    private const uint TypeRelationId = 1247;

    private const string LockNotAvailable = "55P03";

    // ExclusiveLock from storage/lockdefs.h, and LOCKTAG_ADVISORY from storage/lock.h, which shows in pg_locks as an
    // advisory lock, taken with DEFAULT_LOCKMETHOD, whose session locks PostgreSQL lets go of when a transaction aborts.
    private const int ExclusiveLock = 7;
    private const byte LockTagAdvisory = 10;
    private const byte DefaultLockMethod = 1;

    private const uint LsnOid = 3220;

    // How long a kept fence's DDL may take to arrive before apply asks its origin whether it ever will.
    private static readonly TimeSpan s_reconcileAfter = TimeSpan.FromSeconds(10);

    // The fences this transaction asked other nodes for, with the subtransaction that did.
    private static readonly List<(ulong Id, uint NodeId, uint Subxact, List<uint> Relids)> s_heldFences = [];

    // The connection to each node that holds this transaction's fences there until it ends, and the nodes whose
    // connection broke while it held fences the transaction still needs.
    private static readonly Dictionary<uint, PgReplicationConnection> s_fenceConnections = [];
    private static readonly HashSet<uint> s_lostNodes = [];

    // The fences of the DDL this transaction queued, which the other nodes keep until it applies there.
    private static readonly HashSet<ulong> s_queuedFences = [];

    // The fence of a statement that commits on its way, such as CREATE INDEX CONCURRENTLY, which the other nodes keep
    // from its first commit, so that a lost connection doesn't let it go while the statement builds, and which names a
    // lock the statement holds, for the other nodes to ask about, until the transaction that queues its DDL ends; or
    // zero. Every transaction the statement commits or aborts is seen by a callback of its own, registered once, since
    // the statement's commits end the callbacks registered for a transaction.
    private static ulong s_statementLock;
    private static bool s_statementCallbackRegistered;

    // The tables this transaction fenced, which later DDL in it needs no new fence for.
    private static readonly HashSet<uint> s_transactionTables = [];

    // The fences that the DDL this apply transaction ran, or skipped, was for, to let go once it commits.
    private static readonly HashSet<ulong> s_appliedFences = [];

    // How far each kept fence's origin had written once apply learned that the fence's transaction had committed, a
    // point fixed in its WAL that apply passes once it has applied or skipped the DDL.
    private static readonly Dictionary<long, ulong> s_reconcileBounds = [];

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

    [PgNativeCallback(nameof(StatementTransactionEnded))]
    private static partial XactCallback StatementTransactionCallback { get; }

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
        long deadline = Worker.GetCurrentTimestamp() + (timeoutMs * 1000L);

        // worf reads its catalogs for the role that the other node connects as.
        WorfLocalNode? node = WorfNode.GetLocalNode(false, true);
        if (node is null || !Elevated.Run(() => WorfNode.GetNodeSubscriptions(node.Node.Id, false)
            .Exists(sub => sub.Enabled && sub.Origin.Id == originNode && sub.ReplicationSets.Contains("ddl_sql"))))
        {
            return PgDatum.DangerousCreate(0, LsnOid, PgMemoryContext.Current);
        }

        // Writes to a table this node doesn't replicate never reach the other node, so only its DDL is held back. A
        // table's partitions and children here are held back with it.
        List<(uint Relid, bool BlocksWrites)> fenced = [];
        foreach (string? relation in relations)
        {
            SpiResult result = Worf.SelectCatalog("SELECT pg_catalog.to_regclass($1)::oid", SpiParameter.Create(relation));
            if (result.Count == 0 || result[0].Get<uint?>(0) is not uint relid)
            {
                continue;
            }

            List* tables = NativeMethods.find_all_inheritors(relid, (int)PgLockMode.None, null);
            for (int i = 0; tables is not null && i < tables->length; i++)
            {
                uint table = tables->elements[i].oid_value;
                if (!fenced.Exists(fence => fence.Relid == table))
                {
                    fenced.Add((table, Elevated.Run(() => ReplicationSet.GetTableReplicationSets(node.Node.Id, table).Count > 0)));
                }
            }
        }

        // A fence that isn't kept goes with the session that asked for it, as when the other node's DDL fails.
        if (!s_releasesOnExit)
        {
            _ = PgProcess.OnExit(static _ => _ = Release(fence => fence.HolderPid == Environment.ProcessId));
            s_releasesOnExit = true;
        }

        EnsureLoaded();
        ulong id = unchecked((ulong)fenceId);
        _ = AddFences([.. fenced.Select(fence => (id, fence.Relid, fence.BlocksWrites))], Environment.ProcessId, null);
        try
        {
            // A ShareLock waits for the writes already under way. Later ones fail on the fence. Every wait shares the
            // timeout.
            foreach (uint relid in fenced.Where(fence => fence.BlocksWrites).Select(fence => fence.Relid))
            {
                Worker.SetConfigOption("lock_timeout", RemainingMs(deadline).ToString(CultureInfo.InvariantCulture), GucContext.PGC_USERSET);
                NativeMethods.LockRelationOid(relid, (int)PgLockMode.Share);
                NativeMethods.UnlockRelationOid(relid, (int)PgLockMode.Share);
            }
        }
        catch
        {
            _ = Release(fence => fence.Id == id && fence.Dboid == DatabaseId());
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
    /// Lets go of a fence this session asked for and hasn't kept, as when the savepoint of the DDL it was for rolls back,
    /// or one kept for a statement that commits on its way and then failed. A procedure, since the node that asked
    /// calls it as the savepoint or the statement's transaction rolls back, when only commands without rows can run.
    /// </summary>
    [PgFunction(Sql = """
        CREATE PROCEDURE worf.unfence_writes(fence_id bigint)
        LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        REVOKE ALL ON PROCEDURE worf.unfence_writes(bigint) FROM public;
        """)]
    internal static void UnfenceWrites(long fenceId)
    {
        ulong id = unchecked((ulong)fenceId);
        uint dboid = DatabaseId();
        _ = Release(fence => fence.Id == id && fence.Dboid == dboid && fence.HolderPid == Environment.ProcessId);

        // A kept fence goes once its rows have.
        if (Elevated.Run(() => Worf.ReadScalar<bool>("SELECT EXISTS (SELECT FROM worf.fence WHERE fence_id = $1)", SpiParameter.Create(fenceId))))
        {
            Elevated.Run(() => _ = Spi.Execute("DELETE FROM worf.fence WHERE fence_id = $1", SpiParameter.Create(fenceId)));
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, () => _ = Release(fence => fence.Id == id && fence.Dboid == dboid));
        }
    }

    /// <summary>
    /// Keeps a fence this session asked for after the session ends, across restarts, until the change it's for applies
    /// here, or its origin says the transaction that made the change aborted.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.keep_fence(fence_id bigint, origin_node oid, origin_xid bigint)
        RETURNS void STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        REVOKE ALL ON FUNCTION worf.keep_fence(bigint, oid, bigint) FROM public;
        """)]
    internal static void KeepFence(long fenceId, uint originNode, long originXid)
    {
        ulong id = unchecked((ulong)fenceId);
        uint dboid = DatabaseId();
        int pid = Environment.ProcessId;
        List<(uint Relid, bool BlocksWrites)> kept = [];
        using (PgLwLockShareGuard<WorfFenceTable, WorfFence> guard = Fences.Share())
        {
            for (int i = 0; i < guard.Length; i++)
            {
                if (guard[i].Id == id && guard[i].Dboid == dboid && guard[i].HolderPid == pid)
                {
                    kept.Add((guard[i].Relid, guard[i].BlocksWrites));
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

        // The fence outlasts this session once the rows that keep it across restarts have committed.
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, () =>
        {
            using PgLwLockExclusiveGuard<WorfFenceTable, WorfFence> guard = Fences.Exclusive();
            for (int i = 0; i < guard.Length; i++)
            {
                if (guard[i].Id == id && guard[i].Dboid == dboid && guard[i].HolderPid == pid)
                {
                    WorfFence fence = guard[i];
                    fence.HolderPid = 0;
                    guard[i] = fence;
                }
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
        Func<WorfFence, bool> matches = fence => fence.Dboid == dboid && (fenceId is null || fence.Id == unchecked((ulong)fenceId.Value));

        // The fences go once their rows have, so that a release that rolls back keeps them.
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, () => _ = Release(matches));
        int released = 0;
        using PgLwLockShareGuard<WorfFenceTable, WorfFence> guard = Fences.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            released += guard[i].Id != 0 && matches(guard[i]) ? 1 : 0;
        }

        return released;
    }

    /// <summary>
    /// Checks whether this database may hold fences, as only one with worf may.
    /// </summary>
    /// <returns>Whether it may.</returns>
    internal static bool MayHoldFences() => Worf.CatalogOid("fence") != 0;

    /// <summary>
    /// Fails a local write to tables this node holds back, as a COPY's or TRUNCATE's tables, which the caller has
    /// locked.
    /// </summary>
    /// <param name="relids">The tables.</param>
    internal static void CheckWrite(List<uint> relids) => Check(relids, false);

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
    /// transaction block, any DDL that locks out writes locks the tables it touches, since later DDL there can't wait
    /// for other nodes' changes to tables the transaction has locked.
    /// </summary>
    /// <param name="parsetree">The DDL.</param>
    internal static void LockNodes(Node* parsetree)
    {
        s_statementFence = null;
        if (!GlobalDdlLock)
        {
            return;
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
        if (s_subtransactionAbort is null)
        {
            RegisterCallbacks();
        }

        StringBuilder array = new("ARRAY[");
        for (int i = 0; i < relids.Count; i++)
        {
            array.Append(i == 0 ? "" : ", ").Append(Spi.QuoteLiteral(QualifiedName(relids[i])));
        }

        array.Append("]::text[]");

        // Holding back each node's writes and catching up with them all share worf.ddl_lock_timeout.
        long deadline = Worker.GetCurrentTimestamp() + (DdlLockTimeout * 1000L);
        List<(uint NodeId, ulong Lsn)> positions = [];
        uint subxact = NativeMethods.GetCurrentSubTransactionId();
        foreach ((uint nodeId, string dsn) in peers)
        {
            PgReplicationConnection conn = FenceConnection(nodeId, dsn);
            s_heldFences.Add((fenceId, nodeId, subxact, relids));
            string? lsn;
            try
            {
                lsn = conn.Query($"SELECT worf.fence_writes({unchecked((long)fenceId).ToString(CultureInfo.InvariantCulture)}, {node.Node.Id.ToString(CultureInfo.InvariantCulture)}, {array}, {RemainingMs(deadline).ToString(CultureInfo.InvariantCulture)})", 1)[0][0];
            }
            catch (PgRemoteException error)
            {
                throw new PgException(LockNotAvailable, $"could not hold back writes on node {nodeId}: {error.Message}");
            }

            // A node that doesn't receive this node's DDL holds nothing back.
            if (!Worf.TryParseLsn(lsn ?? "", out ulong position) || position == 0)
            {
                s_heldFences.RemoveAt(s_heldFences.Count - 1);
                continue;
            }

            positions.Add((nodeId, position));
        }

        WaitUntilApplied(node.Node.Id, positions, deadline);

        // Another node may have begun DDL on these tables meanwhile, and both give way.
        Check(relids, true);
        s_transactionTables.UnionWith(relids);
        s_statementFence = (fenceId, positions);
    }

    /// <summary>
    /// Takes the fence of the DDL statement that runs now, for its queued message, which the other nodes then keep
    /// until it applies there.
    /// </summary>
    /// <returns>The fence and how far each fenced node's WAL had reached, or null.</returns>
    internal static (ulong Id, List<(uint NodeId, ulong Lsn)> Positions)? TakeStatementFence()
    {
        (ulong Id, List<(uint NodeId, ulong Lsn)> Positions)? fence = s_statementFence;
        s_statementFence = null;
        if (fence is not null && s_heldFences.Exists(held => held.Id == fence.Value.Id))
        {
            _ = s_queuedFences.Add(fence.Value.Id);
        }

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
        => WaitUntilApplied(localNodeId, positions, Worker.GetCurrentTimestamp() + (timeoutMs * 1000L));

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
                List<long> fences = [.. kept.Select(row => row.Get<long>(0))];
                foreach (long forgotten in s_reconcileBounds.Keys.Where(fence => !fences.Contains(fence)).ToList())
                {
                    _ = s_reconcileBounds.Remove(forgotten);
                }

                if (kept.Count == 0)
                {
                    return;
                }

                using PgReplicationConnection conn = Worf.Connect(sub.OriginIf.Dsn, sub.Name, "fences");
                for (int i = 0; i < kept.Count; i++)
                {
                    long fenceId = kept[i].Get<long>(0);

                    // A transaction the origin hasn't reached never ran there, as on an origin restored to an earlier
                    // point, and one too old for the origin to know of finished long ago. A statement that commits on
                    // its way holds a lock named for its fence until the transaction that queues its DDL ends.
                    ulong key = unchecked((ulong)fenceId);
                    string?[] row = conn.Query($"""
                        SELECT CASE WHEN EXISTS (SELECT FROM pg_catalog.pg_locks l JOIN pg_catalog.pg_database d ON d.oid = l.database
                                WHERE l.locktype = 'advisory' AND d.datname = pg_catalog.current_database() AND l.objsubid = 1
                                    AND l.classid = {(key >> 32).ToString(CultureInfo.InvariantCulture)}
                                    AND l.objid = {(key & uint.MaxValue).ToString(CultureInfo.InvariantCulture)})
                            THEN 'in progress'
                            WHEN x >= pg_catalog.pg_snapshot_xmax(pg_catalog.pg_current_snapshot()) THEN 'aborted'
                            ELSE pg_catalog.pg_xact_status(x) END, pg_catalog.pg_current_wal_insert_lsn()
                        FROM (SELECT '{kept[i].Get<long>(1).ToString(CultureInfo.InvariantCulture)}'::pg_catalog.xid8 AS x) AS fence
                        """, 2)[0];
                    if (row[0] == "in progress")
                    {
                        continue;
                    }

                    // The origin's WAL had passed the transaction's commit once the origin said it committed, so once
                    // this node has applied that far, it has applied or skipped the DDL.
                    bool aborted = row[0] == "aborted";
                    if (!aborted && !s_reconcileBounds.ContainsKey(fenceId) && Worf.TryParseLsn(row[1] ?? "", out ulong bound))
                    {
                        s_reconcileBounds[fenceId] = bound;
                    }

                    if (aborted || (s_reconcileBounds.TryGetValue(fenceId, out ulong past) && AppliedPast(sub.Origin.Id, past)))
                    {
                        PgLog.Log($"letting go of fence {fenceId} from node {sub.Origin.Name}, whose DDL {(aborted ? "never committed" : "this node has gone past")}");
                        _ = Spi.Execute("DELETE FROM worf.fence WHERE fence_id = $1", SpiParameter.Create(fenceId));
                        _ = s_reconcileBounds.Remove(fenceId);
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
    /// Waits until this node has applied each node's changes as far as the given WAL position, through every enabled
    /// subscription to it, for the nodes other than itself, failing at once when this transaction holds a lock that
    /// those changes wait for.
    /// </summary>
    private static void WaitUntilApplied(uint localNodeId, List<(uint NodeId, ulong Lsn)> positions, long deadline)
    {
        List<(uint SubId, uint NodeId, ulong Lsn)> waiting = Elevated.Run(() => positions
            .Where(position => position.NodeId != localNodeId)
            .SelectMany(position => WorfNode.GetNodeSubscriptions(position.NodeId, true)
                .Where(sub => sub.Enabled)
                .Select(sub => (sub.Id, position.NodeId, position.Lsn)))
            .ToList());

        for (int polls = 1; waiting.Count > 0; polls++)
        {
            List<long> blocked = [];
            using (PgLwLockShareGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Share())
            {
                _ = waiting.RemoveAll(position =>
                {
                    for (int i = 0; i < guard.Length; i++)
                    {
                        WorfWorker worker = guard[i];
                        if (worker.WorkerType == WorfWorkerType.Apply && worker.Dboid == DatabaseId() &&
                            worker.Worker.Apply.SubId == position.SubId)
                        {
                            if (worker.Worker.Apply.FlushLsn >= position.Lsn)
                            {
                                return true;
                            }

                            if (worker.Proc != 0)
                            {
                                blocked.Add(((PGPROC*)worker.Proc)->pid);
                            }
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

            // Apply can't pass a lock this transaction holds, as on a table that an earlier statement in it locked or
            // wrote to.
            if (polls % 50 == 0 && blocked.Count > 0 && Spi.Select("""
                SELECT EXISTS (SELECT FROM pg_catalog.unnest($1::pg_catalog.int8[]) AS a (pid)
                    WHERE pg_catalog.pg_backend_pid() = ANY (pg_catalog.pg_blocking_pids(a.pid::pg_catalog.int4)))
                """, SpiParameter.Create(blocked.ToArray()))[0].Get<bool>(0))
            {
                throw new PgException(LockNotAvailable,
                    $"cannot apply node {waiting[0].NodeId}'s changes before DDL while this transaction holds a lock they need",
                    hint: "Run the DDL before the statements that lock or write to its tables, or in a transaction of its own.");
            }

            Worker.WaitLatch(10);
            PgInterrupts.Check();
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
    /// Fails a local write, or local DDL, on tables another node holds back, unless apply runs it. A write to a
    /// partitioned table checks the partitions it can reach.
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

        List<uint> tables = ddl ? relids : [.. relids.SelectMany(Partitions)];
        uint dboid = DatabaseId();
        uint held = 0;
        using (PgLwLockShareGuard<WorfFenceTable, WorfFence> guard = Fences.Share())
        {
            for (int i = 0; i < guard.Length && held == 0; i++)
            {
                WorfFence fence = guard[i];
                if (fence.Id != 0 && fence.Dboid == dboid && (ddl || fence.BlocksWrites) && tables.Contains(fence.Relid) &&
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
    /// Lists a table, and for a partitioned table, the partitions under it that tuple routing can write to.
    /// </summary>
    private static List<uint> Partitions(uint relid)
    {
        List<uint> tables = [relid];
        if (NativeMethods.get_rel_relkind(relid) == (sbyte)'p')
        {
            List* inheritors = NativeMethods.find_all_inheritors(relid, (int)PgLockMode.None, null);
            for (int i = 1; inheritors is not null && i < inheritors->length; i++)
            {
                tables.Add(inheritors->elements[i].oid_value);
            }
        }

        return tables;
    }

    /// <summary>
    /// Reads back this database's kept fences once after the server starts, before any write is checked. Reading them
    /// again adds only those it lacks, so the databases noted as read are only a cache, which forgets one with no
    /// fences when it's full.
    /// </summary>
    private static void EnsureLoaded()
    {
        if (s_loaded)
        {
            return;
        }

        uint dboid = DatabaseId();
        long generation;
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

            generation = table.Generation;
        }

        if (!MayHoldFences())
        {
            return;
        }

        // A fence let go of while its row was read would come back, so the rows are read again when one was.
        while (true)
        {
            List<(ulong Id, uint Relid, bool BlocksWrites)> kept = [];
            Elevated.Run(() =>
            {
                SpiResult rows = Worf.SelectCatalog("SELECT fence_id, relid, blocks_writes FROM worf.fence");
                for (int i = 0; i < rows.Count; i++)
                {
                    kept.Add((unchecked((ulong)rows[i].Get<long>(0)), rows[i].Get<uint>(1), rows[i].Get<bool>(2)));
                }
            });

            if (AddFences(kept, 0, generation) is not long newer)
            {
                s_loaded = true;
                return;
            }

            generation = newer;
        }
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
            table.Generation++;
            guard.Header = table;
            _ = PgAtomic.Add(FenceCount, -released);
        }

        return released;
    }

    /// <summary>
    /// Adds fences for this database. Fences read back from worf.fence add only those it lacks, and in the same hold
    /// of the lock note the database as read, unless a fence was let go of since the read.
    /// </summary>
    /// <param name="fences">The fences.</param>
    /// <param name="holderPid">The session that holds them, or zero for kept fences.</param>
    /// <param name="readAt">For fences read back, the generation of releases the read saw, or null.</param>
    /// <returns>The newer generation, when a fence was let go of since the read, or null.</returns>
    private static long? AddFences(List<(ulong Id, uint Relid, bool BlocksWrites)> fences, int holderPid, long? readAt)
    {
        uint dboid = DatabaseId();
        using PgLwLockExclusiveGuard<WorfFenceTable, WorfFence> guard = Fences.Exclusive();
        WorfFenceTable table = guard.Header;
        if (readAt is long generation)
        {
            for (int i = 0; i < table.LoadedCount; i++)
            {
                if (table.Loaded[i] == dboid)
                {
                    return null;
                }
            }

            if (table.Generation != generation)
            {
                return table.Generation;
            }
        }

        int next = 0;
        foreach ((ulong id, uint relid, bool blocksWrites) in fences)
        {
            bool present = false;
            for (int i = 0; readAt is not null && i < guard.Length && !present; i++)
            {
                present = guard[i].Id == id && guard[i].Dboid == dboid && guard[i].Relid == relid;
            }

            if (present)
            {
                continue;
            }

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
                Id = id,
                Dboid = dboid,
                Relid = relid,
                HolderPid = holderPid,
                BlocksWrites = blocksWrites,
            };
            table.Used++;
            _ = PgAtomic.Add(FenceCount, 1);
        }

        // A database with no fences can be read again at no risk, so a full cache forgets one.
        if (readAt is not null)
        {
            int slot = table.LoadedCount < WorfFenceTable.MaxDatabases ? table.LoadedCount++ : -1;
            if (slot < 0)
            {
                HashSet<uint> fenced = [];
                for (int i = 0; i < guard.Length; i++)
                {
                    if (guard[i].Id != 0)
                    {
                        _ = fenced.Add(guard[i].Dboid);
                    }
                }

                for (int i = 0; i < WorfFenceTable.MaxDatabases && slot < 0; i++)
                {
                    slot = fenced.Contains(table.Loaded[i]) ? -1 : i;
                }
            }

            if (slot >= 0)
            {
                table.Loaded[slot] = dboid;
            }
        }

        guard.Header = table;
        return null;
    }

    /// <summary>
    /// Opens the connection that holds this transaction's fences on a node, or returns the one it has opened, which
    /// stays open while the transaction lasts however long it idles. A node whose server notices only late that this
    /// one died keeps its fence that long, so the connection is probed.
    /// </summary>
    private static PgReplicationConnection FenceConnection(uint nodeId, string dsn)
    {
        if (s_fenceConnections.TryGetValue(nodeId, out PgReplicationConnection? open))
        {
            return open;
        }

        PgReplicationConnection conn = Worf.Connect(dsn, "worf_ddl_lock", null);
        s_fenceConnections[nodeId] = conn;
        try
        {
            _ = conn.Query("""
                SELECT pg_catalog.set_config('tcp_keepalives_idle', '10', false), pg_catalog.set_config('tcp_keepalives_interval', '5', false),
                    pg_catalog.set_config('idle_session_timeout', '0', false), pg_catalog.set_config('idle_in_transaction_session_timeout', '0', false)
                """, 4);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(LockNotAvailable, $"could not hold back writes on node {nodeId}: {error.Message}");
        }

        return conn;
    }

    /// <summary>
    /// Gets the milliseconds left before a deadline, failing once it has passed.
    /// </summary>
    private static long RemainingMs(long deadline)
    {
        long remaining = (deadline - Worker.GetCurrentTimestamp()) / 1000;
        return remaining > 0 ? remaining
            : throw new PgException(LockNotAvailable, "timed out holding back other nodes' writes before DDL");
    }

    /// <summary>
    /// Lists the tables whose rows from other nodes the DDL could fail to apply if it ran before them, or the reverse:
    /// a table's partitions, a partition it attaches, a schema's tables, the tables a new foreign key references, and
    /// the tables with columns of a changed domain, enum or composite type. In a transaction block, it lists the tables that any DDL locks
    /// against writes.
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
                AlterTableStmt* alter = (AlterTableStmt*)parsetree;
                AddTable(tables, Relid(alter->relation));
                List* cmds = alter->cmds;
                for (int i = 0; cmds is not null && i < cmds->length; i++)
                {
                    AlterTableCmd* cmd = (AlterTableCmd*)cmds->elements[i].ptr_value;
                    if (cmd->subtype is AlterTableType.AT_AddConstraint or AlterTableType.AT_AddColumn)
                    {
                        AddReferencedTables(tables, cmd->def);
                    }
                    else if (cmd->subtype == AlterTableType.AT_AttachPartition)
                    {
                        AddTable(tables, Relid(((PartitionCmd*)cmd->def)->name));
                    }
                }

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

            case NodeTag.T_CreateTrigStmt when inBlock:
                AddTable(tables, Relid(((CreateTrigStmt*)parsetree)->relation));
                break;

            case NodeTag.T_RuleStmt when inBlock:
                AddTable(tables, Relid(((RuleStmt*)parsetree)->relation));
                break;

            case NodeTag.T_CreatePolicyStmt when inBlock:
                AddTable(tables, Relid(((CreatePolicyStmt*)parsetree)->table));
                break;

            case NodeTag.T_AlterPolicyStmt when inBlock:
                AddTable(tables, Relid(((AlterPolicyStmt*)parsetree)->table));
                break;

            // A new partition locks its parent, and a foreign key the tables it references.
            case NodeTag.T_CreateStmt when inBlock:
                CreateStmt* create = (CreateStmt*)parsetree;
                if (create->partbound is not null && create->inhRelations is not null)
                {
                    AddTable(tables, Relid((RangeVar*)create->inhRelations->elements[0].ptr_value));
                }

                List* elements = create->tableElts;
                for (int i = 0; elements is not null && i < elements->length; i++)
                {
                    AddReferencedTables(tables, (Node*)elements->elements[i].ptr_value);
                }

                for (int i = 0; create->constraints is not null && i < create->constraints->length; i++)
                {
                    AddReferencedTables(tables, (Node*)create->constraints->elements[i].ptr_value);
                }

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
                    List* names = (List*)objects->elements[i].ptr_value;
                    switch (drop->removeType)
                    {
                        case ObjectType.OBJECT_TABLE:
                            AddTable(tables, Relid(NativeMethods.makeRangeVarFromNameList(names)));
                            break;

                        case ObjectType.OBJECT_SCHEMA when drop->behavior == DropBehavior.DROP_CASCADE:
                            AddSchema(tables, Worf.FromServer(NativeMethods.NameListToString(NativeMethods.lappend(null, names))));
                            break;

                        case ObjectType.OBJECT_INDEX when inBlock:
                            uint index = Relid(NativeMethods.makeRangeVarFromNameList(names));
                            AddTable(tables, index == 0 ? 0 : NativeMethods.IndexGetRelation(index, true));
                            break;

                        case ObjectType.OBJECT_TRIGGER or ObjectType.OBJECT_RULE or ObjectType.OBJECT_POLICY when inBlock:
                            AddTable(tables, Relid(TableOf(names)));
                            break;

                        case ObjectType.OBJECT_TYPE or ObjectType.OBJECT_DOMAIN when inBlock && drop->behavior == DropBehavior.DROP_CASCADE:
                            AddTypeUsers(tables, TypeOid(((TypeName*)names)->names));
                            break;

                        default:
                            break;
                    }
                }

                break;

            default:
                break;
        }

        return [.. tables.Distinct()];
    }

    /// <summary>
    /// Adds the table that a new foreign key, as a table constraint or a column's, references.
    /// </summary>
    private static void AddReferencedTables(List<uint> tables, Node* element)
    {
        if (element is null)
        {
            return;
        }

        if (element->type == NodeTag.T_Constraint && ((Constraint*)element)->contype == ConstrType.CONSTR_FOREIGN)
        {
            AddTable(tables, Relid(((Constraint*)element)->pktable));
        }
        else if (element->type == NodeTag.T_ColumnDef)
        {
            List* constraints = ((ColumnDef*)element)->constraints;
            for (int i = 0; constraints is not null && i < constraints->length; i++)
            {
                AddReferencedTables(tables, (Node*)constraints->elements[i].ptr_value);
            }
        }
    }

    /// <summary>
    /// Checks whether every part of an ALTER TABLE leaves rows from other nodes that don't have it yet applying as
    /// they did, as adding a column that can be missing, dropping a column or a constraint, detaching a partition,
    /// whose rows apply to it alone either way, or changing settings do.
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
                    AlterTableType.AT_GenericOptions or AlterTableType.AT_DetachPartition or
                    AlterTableType.AT_DetachPartitionFinalize => true,
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
    /// Checks whether the user may run the DDL, as PostgreSQL will check, so that someone who may not doesn't hold
    /// back other nodes: by owning its table, schema or type, by holding the privilege a trigger or foreign key
    /// needs, or, for a DROP, by owning the dropped object's schema.
    /// </summary>
    private static bool MayRun(Node* parsetree)
    {
        uint user = NativeMethods.GetUserId();
        switch (parsetree->type)
        {
            case NodeTag.T_AlterTableStmt:
                return Owns(RelationRelationId, Relid(((AlterTableStmt*)parsetree)->relation), user);

            case NodeTag.T_RenameStmt when ((RenameStmt*)parsetree)->renameType == ObjectType.OBJECT_SCHEMA:
                return Owns(NamespaceRelationId, NamespaceOid(Worf.FromServer(((RenameStmt*)parsetree)->subname)), user);

            case NodeTag.T_RenameStmt:
                return Owns(RelationRelationId, Relid(((RenameStmt*)parsetree)->relation), user);

            case NodeTag.T_AlterObjectSchemaStmt:
                return Owns(RelationRelationId, Relid(((AlterObjectSchemaStmt*)parsetree)->relation), user);

            case NodeTag.T_IndexStmt:
                return Owns(RelationRelationId, Relid(((IndexStmt*)parsetree)->relation), user);

            case NodeTag.T_CreateTrigStmt:
                uint triggered = Relid(((CreateTrigStmt*)parsetree)->relation);
                return triggered != 0 && NativeMethods.pg_class_aclcheck(triggered, user, AclTrigger) == AclResult.ACLCHECK_OK;

            case NodeTag.T_RuleStmt:
                return Owns(RelationRelationId, Relid(((RuleStmt*)parsetree)->relation), user);

            case NodeTag.T_CreatePolicyStmt:
                return Owns(RelationRelationId, Relid(((CreatePolicyStmt*)parsetree)->table), user);

            case NodeTag.T_AlterPolicyStmt:
                return Owns(RelationRelationId, Relid(((AlterPolicyStmt*)parsetree)->table), user);

            case NodeTag.T_CreateStmt:
                CreateStmt* create = (CreateStmt*)parsetree;
                if (create->partbound is not null && create->inhRelations is not null &&
                    !Owns(RelationRelationId, Relid((RangeVar*)create->inhRelations->elements[0].ptr_value), user))
                {
                    return false;
                }

                List<uint> referenced = [];
                for (int i = 0; create->tableElts is not null && i < create->tableElts->length; i++)
                {
                    AddReferencedTables(referenced, (Node*)create->tableElts->elements[i].ptr_value);
                }

                for (int i = 0; create->constraints is not null && i < create->constraints->length; i++)
                {
                    AddReferencedTables(referenced, (Node*)create->constraints->elements[i].ptr_value);
                }

                return referenced.TrueForAll(relid => NativeMethods.pg_class_aclcheck(relid, user, AclReferences) == AclResult.ACLCHECK_OK ||
                    NativeMethods.pg_attribute_aclcheck_all(relid, user, AclReferences, AclMaskHow.ACLMASK_ANY) == AclResult.ACLCHECK_OK);

            case NodeTag.T_AlterDomainStmt:
                return Owns(TypeRelationId, TypeOid(((AlterDomainStmt*)parsetree)->typeName), user);

            case NodeTag.T_AlterEnumStmt:
                return Owns(TypeRelationId, TypeOid(((AlterEnumStmt*)parsetree)->typeName), user);

            case NodeTag.T_DropStmt:
                DropStmt* drop = (DropStmt*)parsetree;
                List* objects = drop->objects;
                for (int i = 0; objects is not null && i < objects->length; i++)
                {
                    List* names = (List*)objects->elements[i].ptr_value;
                    bool may = drop->removeType switch
                    {
                        ObjectType.OBJECT_SCHEMA => Owns(NamespaceRelationId,
                            NamespaceOid(Worf.FromServer(NativeMethods.NameListToString(NativeMethods.lappend(null, names)))), user),
                        ObjectType.OBJECT_TABLE or ObjectType.OBJECT_INDEX => OwnsOrItsSchema(RelationRelationId,
                            Relid(NativeMethods.makeRangeVarFromNameList(names)), user),
                        ObjectType.OBJECT_TRIGGER or ObjectType.OBJECT_RULE or ObjectType.OBJECT_POLICY =>
                            Owns(RelationRelationId, Relid(TableOf(names)), user),
                        ObjectType.OBJECT_TYPE or ObjectType.OBJECT_DOMAIN => OwnsOrItsSchema(TypeRelationId,
                            TypeOid(((TypeName*)names)->names), user),
                        _ => true,
                    };
                    if (!may)
                    {
                        return false;
                    }
                }

                return true;

            default:
                return false;
        }
    }

    /// <summary>
    /// Checks whether the user owns an object, or the schema it's in, which lets them drop it.
    /// </summary>
    private static bool OwnsOrItsSchema(uint classId, uint objectId, uint user)
    {
        if (objectId == 0)
        {
            return true;
        }

        ObjectAddress address = new() { classId = classId, objectId = objectId };
        return Owns(classId, objectId, user) || Owns(NamespaceRelationId, NativeMethods.get_object_namespace(&address), user);
    }

    private static bool Owns(uint classId, uint objectId, uint user)
    {
        if (objectId == 0)
        {
            return false;
        }

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

    /// <summary>
    /// Gets the table of a trigger, rule or policy from its qualified name, whose last part is its own.
    /// </summary>
    private static RangeVar* TableOf(List* names)
        => names is null || names->length < 2 ? null
            : NativeMethods.makeRangeVarFromNameList(NativeMethods.list_truncate(NativeMethods.list_copy(names), names->length - 1));

    private static uint NamespaceOid(string name) => NativeMethods.get_namespace_oid(Worf.PStrDup(name), true);

    private static uint TypeOid(List* names)
        => names is null ? 0 : Worf.ReadScalar<uint?>("SELECT pg_catalog.to_regtype($1)::oid",
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

    /// <summary>
    /// Adds the tables with a column of a type, or of a type built on it: a domain over it, an array, range or
    /// multirange of it, or a composite type, or a table's row type, with an attribute of it, however deep.
    /// </summary>
    private static void AddTypeUsers(List<uint> tables, uint typeOid)
    {
        if (typeOid == 0)
        {
            return;
        }

        SpiResult result = Worf.SelectCatalog("""
            WITH RECURSIVE built (base, derived) AS (
                SELECT typbasetype, oid FROM pg_catalog.pg_type WHERE typtype = 'd'
                UNION ALL SELECT oid, typarray FROM pg_catalog.pg_type WHERE typarray <> 0
                UNION ALL SELECT a.atttypid, c.reltype FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                    WHERE c.reltype <> 0 AND a.attnum > 0 AND NOT a.attisdropped
                UNION ALL SELECT rngsubtype, rngtypid FROM pg_catalog.pg_range
                UNION ALL SELECT rngtypid, rngmultitypid FROM pg_catalog.pg_range
            ), types (oid) AS (
                SELECT $1::pg_catalog.oid
                UNION SELECT built.derived FROM built JOIN types ON built.base = types.oid
            )
            SELECT DISTINCT a.attrelid FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            WHERE a.atttypid IN (SELECT oid FROM types) AND NOT a.attisdropped AND c.relkind IN ('r', 'p')
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
    /// Keeps the fences of the queued DDL when the transaction commits or is prepared, closes their connections once it
    /// ends, and in a subtransaction that rolls back, lets its fences go.
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
            List<(ulong Id, uint NodeId, uint Subxact, List<uint> Relids)> aborted = s_heldFences.FindAll(fence => fence.Subxact == subxact.Value);
            foreach ((ulong Id, uint NodeId, uint Subxact, List<uint> Relids) fence in aborted)
            {
                _ = s_heldFences.Remove(fence);
                s_transactionTables.ExceptWith(fence.Relids);
                if (s_statementFence?.Id == fence.Id)
                {
                    s_statementFence = null;
                }
            }

            foreach (IGrouping<uint, (ulong Id, uint NodeId, uint Subxact, List<uint> Relids)> node in aborted.GroupBy(fence => fence.NodeId))
            {
                Unfence(node.Key, [.. node.Select(fence => fence.Id).Distinct()]);
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

    /// <summary>
    /// Lets a node's fences go while the transaction holds others there. A connection that fails to is closed, which
    /// lets them all go, so the transaction can't commit.
    /// </summary>
    private static void Unfence(uint nodeId, List<ulong> fenceIds)
    {
        if (!s_fenceConnections.TryGetValue(nodeId, out PgReplicationConnection? conn))
        {
            return;
        }

        try
        {
            foreach (ulong id in fenceIds)
            {
                conn.Execute($"CALL worf.unfence_writes({unchecked((long)id).ToString(CultureInfo.InvariantCulture)})");
            }
        }
        catch (Exception error) when (error is PgException or PgRemoteException)
        {
            Close(conn);
            _ = s_fenceConnections.Remove(nodeId);
            if (s_heldFences.Exists(fence => fence.NodeId == nodeId))
            {
                _ = s_lostNodes.Add(nodeId);
            }
        }
    }

    /// <summary>
    /// Ends a statement that committed on its way, once it has run: the transaction that queued its DDL lets the
    /// statement's lock go as it commits, and the other nodes its fence once the DDL applies there, while a statement
    /// that ran without queuing its DDL lets its fence go now.
    /// </summary>
    /// <param name="queued">Whether the statement queued its DDL.</param>
    internal static void EndStatement(bool queued)
    {
        if (s_statementLock != 0 && !queued)
        {
            EndStatementHold(false, true);
        }
    }

    /// <summary>
    /// Keeps the fences of the DDL the transaction queued, under the transaction that queued it, which the other nodes
    /// ask about if the DDL doesn't arrive. The other fences go with their connections. A statement that commits on
    /// its way, such as CREATE INDEX CONCURRENTLY, keeps its fence at its first commit instead.
    /// </summary>
    private static void KeepHeldFences()
    {
        if (s_lostNodes.Count > 0)
        {
            throw new PgException(LockNotAvailable, $"lost the hold on node {s_lostNodes.First()}'s writes for this transaction's DDL");
        }

        if (DdlReplication.Running && s_statementFence is not null)
        {
            if (s_statementLock == 0)
            {
                KeepStatementFence(s_statementFence.Value.Id);
            }

            return;
        }

        Keep([.. s_heldFences
            .Where(fence => s_queuedFences.Contains(fence.Id))
            .Select(fence => (fence.Id, fence.NodeId))
            .Distinct()]);
    }

    /// <summary>
    /// Keeps the fence of a statement that commits on its way, at its first commit, on each node that holds it, and
    /// takes the lock named for it, which PostgreSQL lets go of if a transaction of the statement aborts.
    /// </summary>
    private static void KeepStatementFence(ulong fenceId)
    {
        List<(ulong Id, uint NodeId)> kept = [.. s_heldFences
            .Where(fence => fence.Id == fenceId)
            .Select(fence => (fence.Id, fence.NodeId))
            .Distinct()];
        if (kept.Count == 0)
        {
            return;
        }

        if (!s_statementCallbackRegistered)
        {
            NativeMethods.RegisterXactCallback(StatementTransactionCallback, null);
            s_statementCallbackRegistered = true;
        }

        LOCKTAG tag = StatementLockTag(fenceId);
        _ = NativeMethods.LockAcquire(&tag, ExclusiveLock, true, false);
        s_statementLock = fenceId;
        Keep(kept);
    }

    /// <summary>
    /// Keeps fences on the nodes that hold them, under this transaction.
    /// </summary>
    private static void Keep(List<(ulong Id, uint NodeId)> fences)
    {
        if (fences.Count == 0)
        {
            return;
        }

        string nodeId = WorfNode.GetLocalNode(false, false)!.Node.Id.ToString(CultureInfo.InvariantCulture);
        string xid = Spi.ExecuteScalar<long>("SELECT pg_catalog.txid_current()").ToString(CultureInfo.InvariantCulture);
        foreach ((ulong fenceId, uint node) in fences)
        {
            try
            {
                _ = s_fenceConnections[node].Query($"SELECT worf.keep_fence({unchecked((long)fenceId).ToString(CultureInfo.InvariantCulture)}, {nodeId}, {xid})", 1);
            }
            catch (Exception error) when (error is PgException or PgRemoteException)
            {
                throw new PgException(LockNotAvailable,
                    $"lost the hold on node {node}'s writes for this transaction's DDL: {error.Message.Split('\n')[0]}");
            }
        }
    }

    /// <summary>
    /// Sees each transaction of a statement that commits on its way end: once one aborts, the statement lets its fence
    /// go, and once the transaction that queued its DDL commits, its lock.
    /// </summary>
    private static void StatementTransactionEnded(XactEvent xactEvent, void* arg)
    {
        if (s_statementLock == 0)
        {
            return;
        }

        if (xactEvent == XactEvent.XACT_EVENT_ABORT)
        {
            EndStatementHold(true, true);
        }
        else if (xactEvent == XactEvent.XACT_EVENT_COMMIT && !DdlReplication.Running)
        {
            EndStatementHold(false, false);
        }
    }

    /// <summary>
    /// Ends the hold of a statement that committed on its way: lets its lock go, unless its transaction aborted, which
    /// let it go, and its fence on the other nodes, unless its DDL was queued, and closes its connections. A node that
    /// can't be told lets the fence go once it finds the lock gone.
    /// </summary>
    private static void EndStatementHold(bool aborted, bool unfence)
    {
        ulong fenceId = s_statementLock;
        s_statementLock = 0;
        if (!aborted)
        {
            LOCKTAG tag = StatementLockTag(fenceId);
            _ = NativeMethods.LockRelease(&tag, ExclusiveLock, true);
        }

        if (unfence)
        {
            foreach (PgReplicationConnection conn in s_fenceConnections.Values)
            {
                try
                {
                    conn.Execute($"CALL worf.unfence_writes({unchecked((long)fenceId).ToString(CultureInfo.InvariantCulture)})");
                }
                catch (Exception error) when (error is PgException or PgRemoteException)
                {
                    PgLog.Debug1($"could not let go of fence {fenceId}: {error.Message}");
                }
            }
        }

        CloseConnections();
    }

    /// <summary>
    /// Names the lock of a statement's fence, as <c>pg_advisory_lock</c> names one for a bigint key, which shows in
    /// <c>pg_locks</c> with the key's halves as its class and object.
    /// </summary>
    private static LOCKTAG StatementLockTag(ulong fenceId) => new()
    {
        locktag_field1 = DatabaseId(),
        locktag_field2 = (uint)(fenceId >> 32),
        locktag_field3 = (uint)fenceId,
        locktag_field4 = 1,
        locktag_type = LockTagAdvisory,
        locktag_lockmethodid = DefaultLockMethod,
    };

    private static void CloseHeldFences()
    {
        // A statement that commits on its way keeps its connections through its commits, and still queues its DDL with
        // its fence.
        if (s_statementLock == 0)
        {
            CloseConnections();
        }

        s_subtransactionAbort?.Dispose();
        s_subtransactionCommit?.Dispose();
        s_subtransactionAbort = null;
        s_subtransactionCommit = null;
    }

    /// <summary>
    /// Closes the connections that hold this transaction's fences, which the other nodes drop unless they're kept.
    /// </summary>
    private static void CloseConnections()
    {
        foreach (PgReplicationConnection conn in s_fenceConnections.Values)
        {
            Close(conn);
        }

        s_fenceConnections.Clear();
        s_heldFences.Clear();
        s_lostNodes.Clear();
        s_queuedFences.Clear();
        s_transactionTables.Clear();
        if (!DdlReplication.Running)
        {
            s_statementFence = null;
        }
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
