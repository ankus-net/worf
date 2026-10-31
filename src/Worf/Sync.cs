using System.ComponentModel;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using Ankus.Postgres;
using Path = System.IO.Path;

[assembly: PgSql("local-sync-status-table", """
    CREATE TABLE worf.local_sync_status (
        sync_kind "char" NOT NULL CHECK (sync_kind IN ('i', 's', 'd', 'f')),
        sync_subid oid NOT NULL REFERENCES worf.subscription(sub_id),
        sync_nspname name,
        sync_relname name,
        sync_status "char" NOT NULL,
    	sync_statuslsn pg_lsn NOT NULL,
        UNIQUE (sync_subid, sync_nspname, sync_relname)
    );
    """, Requires = ["node-tables"])]

namespace Worf;

/// <summary>
/// Table synchronization functions.
/// </summary>
internal static class Sync
{
    internal const char SyncKindInit = 'i';
    internal const char SyncKindFull = 'f';
    internal const char SyncKindStructure = 's';
    internal const char SyncKindDataOnly = 'd';

    internal const char SyncStatusNone = '\0';        // No sync.
    internal const char SyncStatusInit = 'i';         // Ask for sync.
    internal const char SyncStatusStructure = 's';    // Sync structure
    internal const char SyncStatusData = 'd';         // Data sync.
    internal const char SyncStatusConstraints = 'c';  // Constraint sync (post-data structure).
    internal const char SyncStatusSyncwait = 'w';     // Table sync is waiting to get OK from main thread.
    internal const char SyncStatusCatchup = 'u';      // Catching up.
    internal const char SyncStatusSyncdone = 'y';     // Synchronization finished (at lsn).
    internal const char SyncStatusReady = 'r';        // Done.

    private const string PgdumpBinary = "pg_dump";
    private const string PgrestoreBinary = "pg_restore";

    /// <summary>
    /// Whether a kind of synchronization copies data.
    /// </summary>
    /// <param name="kind">The kind.</param>
    /// <returns>Whether it copies data.</returns>
    internal static bool SyncKindData(char kind) => kind == SyncKindDataOnly || kind == SyncKindFull;

    /// <summary>
    /// Whether a kind of synchronization copies structure.
    /// </summary>
    /// <param name="kind">The kind.</param>
    /// <returns>Whether it copies structure.</returns>
    internal static bool SyncKindStructureCopy(char kind) => kind == SyncKindStructure || kind == SyncKindFull;

    /// <summary>
    /// Synchronizes the subscription's structure and data, as its status asks.
    /// </summary>
    /// <param name="sub">The subscription.</param>
    /// <returns>Whether the subscription is catching up, to be marked ready once its changes stream.</returns>
    internal static bool SyncSubscription(WorfSubscription sub)
    {
        WorfSyncStatus sync = PgBackgroundWorker.RunTransaction(() => GetSubscriptionSyncStatus(sub.Id, false)!);
        ulong lsn = 0;
        char status = sync.Status;

        switch (status)
        {
            // Already synced, nothing to do except cleanup.
            case SyncStatusReady:
                return false;

            // We can recover from crashes during these.
            case SyncStatusInit:
            case SyncStatusCatchup:
                break;

            default:
                throw new PgException(PgSqlStates.InternalError,
                    $"subscriber {sub.Name} initialization failed during nonrecoverable step ({status}), please try the setup again");
        }

        if (status == SyncStatusInit)
        {
            const bool useFailoverSlot = false;

            PgLog.Info($"initializing subscriber {sub.Name}");

            // The provider's managed sequences are joined before any rows arrive.
            List<WorfManagedSequence> managedSequences = PgBackgroundWorker.RunTransaction(() => SequenceRangeSync.FetchManaged(sub.OriginIf.Dsn, sub.Name));

            using PgReplicationConnection originConnRepl = Worf.ConnectReplica(sub.OriginIf.Dsn, sub.Name, "snap");

            string snapshot = EnsureReplicationSlotSnapshot(sub, originConnRepl, "snap", useFailoverSlot, out lsn);

            try
            {
                string tmpfile = Path.GetFullPath(Path.Join(Worf.TempDirectory, $"worf-{Environment.ProcessId}.dump"));

                try
                {
                    PgBackgroundWorker.RunTransaction(() =>
                    {
                        ushort originid = EnsureReplicationOrigin(sub);
                        ulong xactLastCommitEnd;
                        unsafe
                        {
                            xactLastCommitEnd = NativeGlobals.XactLastCommitEnd;
                        }

                        PgLog.Debug3($"advancing origin with oid {originid} for forwarded row to {Worf.FormatLsn(xactLastCommitEnd)} during subscription sync");
                        PgReplicationOrigin.Advance(originid, new PgLsn(lsn), new PgLsn(xactLastCommitEnd), true, true);
                    });

                    if (SyncKindStructureCopy(sync.Kind))
                    {
                        PgLog.Info("synchronizing structure");

                        PgBackgroundWorker.RunTransaction(() =>
                        {
                            SetSubscriptionSyncStatus(sub.Id, SyncStatusStructure);
                            SequenceRangeSync.RecordForRestore(managedSequences);
                        });

                        // Dump structure to temp storage.
                        DumpStructure(sub, tmpfile, snapshot);

                        // Restore base pre-data structure (types, tables, etc).
                        RestoreStructure(sub, tmpfile, "pre-data");
                    }

                    // Copy data.
                    if (SyncKindData(sync.Kind))
                    {
                        PgLog.Info("synchronizing data");

                        PgBackgroundWorker.RunTransaction(() =>
                        {
                            SetSubscriptionSyncStatus(sub.Id, SyncStatusData);
                            SequenceRangeSync.JoinBeforeCopy(managedSequences, 0);
                        });

                        List<WorfRemoteRel> tables = CopyReplicationSetsData(sub.Name, sub.OriginIf.Dsn, snapshot,
                            sub.ReplicationSets, sub.SlotName);

                        // Store info about all the synchronized tables.
                        PgBackgroundWorker.RunTransaction(() =>
                        {
                            foreach (WorfRemoteRel remoterel in tables)
                            {
                                WorfSyncStatus? oldsync = GetTableSyncStatus(sub.Id, remoterel.Nspname, remoterel.Relname, true);
                                if (oldsync is not null)
                                {
                                    SetTableSyncStatus(sub.Id, remoterel.Nspname, remoterel.Relname, SyncStatusReady, lsn);
                                }
                                else
                                {
                                    CreateLocalSyncStatus(new WorfSyncStatus
                                    {
                                        Kind = SyncKindFull,
                                        Subid = sub.Id,
                                        Nspname = remoterel.Nspname,
                                        Relname = remoterel.Relname,
                                        Status = SyncStatusReady,
                                        Statuslsn = lsn,
                                    });
                                }
                            }
                        });
                    }

                    // Restore post-data structure (indexes, constraints, etc).
                    if (SyncKindStructureCopy(sync.Kind))
                    {
                        PgLog.Info("synchronizing constraints");

                        PgBackgroundWorker.RunTransaction(() => SetSubscriptionSyncStatus(sub.Id, SyncStatusConstraints));

                        RestoreStructure(sub, tmpfile, "post-data");
                        PgBackgroundWorker.RunTransaction(SequenceRangeSync.ClearRestore);
                    }
                }
                finally
                {
                    SyncTmpfileCleanup(tmpfile);
                }
            }
            catch (Exception) when (CleanupAfterError(sub))
            {
                throw;
            }

            status = SyncStatusCatchup;
            PgBackgroundWorker.RunTransaction(() => SetSubscriptionSyncStatus(sub.Id, SyncStatusCatchup));
        }

        return status == SyncStatusCatchup;
    }

    /// <summary>
    /// Marks a subscription that has caught up ready, once its apply streams changes, so that a caller waiting for
    /// the synchronization to complete finds the provider's slot in use.
    /// </summary>
    /// <param name="sub">The subscription.</param>
    internal static void FinishSubscriptionSync(WorfSubscription sub)
    {
        PgBackgroundWorker.RunTransaction(() => SetSubscriptionSyncStatus(sub.Id, SyncStatusReady));

        PgLog.Info($"finished synchronization of subscriber {sub.Name}, ready to enter normal replication");
    }

    /// <summary>
    /// Synchronizes one table's data.
    /// </summary>
    /// <param name="sub">The subscription, with the table's slot name.</param>
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    /// <param name="statusLsn">The position the table's data is consistent with.</param>
    /// <returns>The table's resulting status.</returns>
    internal static char SyncTable(WorfSubscription sub, string nspname, string relname, out ulong statusLsn)
    {
        (char currentStatus, ulong currentLsn) = PgBackgroundWorker.RunTransaction(() =>
        {
            // Sanity check.
            WorfSyncStatus sync = GetSubscriptionSyncStatus(sub.Id, false)!;
            if (sync.Status != SyncStatusReady)
            {
                throw new PgException(PgSqlStates.InternalError,
                    $"subscriber {sub.Name} is not ready, cannot synchronzie individual tables");
            }

            // Check current state of the table.
            sync = GetTableSyncStatus(sub.Id, nspname, relname, false)!;

            // Already synchronized, nothing to do here.
            if (sync.Status is SyncStatusReady or SyncStatusSyncdone)
            {
                return (sync.Status, sync.Statuslsn);
            }

            // If previous sync attempt failed, we need to start from beginning.
            if (sync.Status != SyncStatusInit)
            {
                SetTableSyncStatus(sub.Id, nspname, relname, SyncStatusInit, 0);
            }

            return (SyncStatusNone, sync.Statuslsn);
        });

        statusLsn = currentLsn;
        if (currentStatus != SyncStatusNone)
        {
            return currentStatus;
        }

        List<WorfManagedSequence> managedSequences = PgBackgroundWorker.RunTransaction(() => SequenceRangeSync.FetchManaged(sub.OriginIf.Dsn, sub.Name));
        using PgReplicationConnection originConnRepl = Worf.ConnectReplica(sub.OriginIf.Dsn, sub.Name, "copy");

        string snapshot = EnsureReplicationSlotSnapshot(sub, originConnRepl, "copy_slot", false, out ulong lsn);

        statusLsn = lsn;

        // Make sure we cleanup the slot if something goes wrong.
        try
        {
            PgBackgroundWorker.RunTransaction(() =>
            {
                // The table's managed sequences are joined before any rows arrive.
                SequenceRangeSync.JoinBeforeCopy(managedSequences, Worf.ReadScalar<uint?>("SELECT to_regclass($1)::oid",
                    SpiParameter.Create(Spi.QuoteQualifiedIdentifier(nspname, relname))) ?? 0);

                ushort originid = EnsureReplicationOrigin(sub);
                ulong xactLastCommitEnd;
                unsafe
                {
                    xactLastCommitEnd = NativeGlobals.XactLastCommitEnd;
                }

                PgLog.Debug2($"advancing origin {Apply.MySubscription!.SlotName} (oid {originid}) for forwarded row to {Worf.FormatLsn(xactLastCommitEnd)} after sync error");
                PgReplicationOrigin.Advance(originid, new PgLsn(lsn), new PgLsn(xactLastCommitEnd), true, true);

                SetTableSyncStatus(sub.Id, nspname, relname, SyncStatusData, lsn);
            });

            // Copy data.
            CopyTablesData(sub.Name, sub.OriginIf.Dsn, snapshot, [(nspname, relname)], sub.ReplicationSets, sub.SlotName);
        }
        catch (Exception) when (CleanupAfterError(sub))
        {
            throw;
        }

        return SyncStatusSyncwait;
    }

    /// <summary>
    /// The sync worker's entry point.
    /// </summary>
    /// <param name="mainArg">The worker's slot.</param>
    [PgBackgroundWorker(EntryPoint = "worf_sync_main")]
    internal static void SyncMain(nuint mainArg)
    {
        int slot = (int)mainArg;

        // Setup shmem.
        Worker.WorkerAttach(slot, WorfWorkerType.Sync);
        WorfSyncWorker mySyncWorker = Worker.MyWorker.Worker.Sync;
        Apply.MyApplyWorkerSlot = slot;

        // Setup synchronous commit according to the user's wishes
        Worker.SetConfigOption("synchronous_commit", Worf.SynchronousCommit ? "local" : "off");

        // Run as replica session replication role.
        Worker.SetConfigOption("session_replication_role", "replica", GucContext.PGC_SUSET);

        // Disable function body checks during replay. That's necessary because a) the creator of the function might
        // have had it disabled b) the function might be search_path dependant and we don't fix the contents of
        // functions.
        Worker.SetConfigOption("check_function_bodies", "off", GucContext.PGC_INTERNAL);

        WorfSubscription sub = PgBackgroundWorker.RunTransaction(() => WorfNode.GetSubscription(mySyncWorker.Apply.SubId));
        Apply.MySubscription = sub;

        string nspname = Worker.NameStr(mySyncWorker.Nspname);
        string relname = Worker.NameStr(mySyncWorker.Relname);
        string tablename = Spi.QuoteQualifiedIdentifier(nspname, relname);

        sub.SlotName = $"{sub.SlotName}_{Worf.HashAny(tablename):x8}";

        PgLog.Log($"starting sync of table {nspname}.{relname} for subscriber {sub.Name}");
        PgLog.Debug1($"connecting to provider {sub.OriginIf.Name}, dsn {sub.OriginIf.Dsn}");

        // Do the initial sync first.
        char status = SyncTable(sub, nspname, relname, out ulong statusLsn);
        if (status is SyncStatusSyncdone or SyncStatusReady)
        {
            SyncWorkerFinish(nspname, relname);
            return;
        }

        // Wait for ack from the main apply thread.
        PgBackgroundWorker.RunTransaction(() => SetTableSyncStatus(sub.Id, nspname, relname, SyncStatusSyncwait, statusLsn));

        // Signal the apply worker to check syncing tables. Without this, the apply worker may not notice the SYNCWAIT
        // status until its next timeout (up to 1 second), which can cause unnecessary delays.
        uint dboid = Worker.MyWorker.Dboid;
        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
        {
            int apply = guard.Read((in _, workers) =>
                Worker.ApplyFind(workers, dboid, mySyncWorker.Apply.SubId));
            if (apply >= 0 && guard[apply].Proc != 0)
            {
                WorfWorker worker = guard[apply];
                worker.Worker.Apply.SyncPending = true;
                guard[apply] = worker;
                Worker.SetProcLatch(worker.Proc);
            }
        }

        _ = WaitForSyncStatusChange(sub.Id, nspname, relname, SyncStatusCatchup, out _);

        // Setup the origin and get the starting position for the replication.
        bool done = PgBackgroundWorker.RunTransaction(() =>
        {
            ushort originid = PgReplicationOrigin.Find(sub.SlotName)
                ?? throw new PgException(PgSqlStates.UndefinedObject, $"replication origin \"{sub.SlotName}\" does not exist");
            PgLog.Debug2($"setting origin {sub.SlotName} (oid {originid}) for subscription sync");
            PgReplicationOrigin.SetupSession(originid);

            // In case there is nothing to catchup, finish immediately.
            if (statusLsn >= Worker.MyWorker.Worker.Sync.Apply.ReplayStopLsn)
            {
                // Mark local table as done.
                SetTableSyncStatus(mySyncWorker.Apply.SubId, nspname, relname, SyncStatusSyncdone, statusLsn);
                return true;
            }

            return false;
        });

        if (done)
        {
            SyncWorkerFinish(nspname, relname);
            return;
        }

        // Start the replication.
        using (PgReplicationConnection streamConn = Worf.ConnectReplica(sub.OriginIf.Dsn, sub.Name, "catchup"))
        {
            // IDENTIFY_SYSTEM sets up some internal state on walsender so call it even if we don't (yet) want to use
            // any of the results.
            _ = PgBackgroundWorker.RunTransaction(() => Worf.IdentifySystem(streamConn));

            Worf.StartReplication(streamConn, sub.SlotName, statusLsn, "all", null, tablename, sub.ForceTextTransfer);

            // Leave it to standard apply code to do the replication.
            Apply.ApplyWork(streamConn);
        }

        // We should only get here if we received sigTERM, which in case of sync worker is not expected.
        Worker.ProcExit(1);
    }

    /// <summary>
    /// Finishes the sync worker's work.
    /// </summary>
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    internal static void SyncWorkerFinish(string nspname, string relname)
    {
        WorfSubscription sub = Apply.MySubscription!;

        // Any outstanding transaction committed when its callback returned, and published its statistics.

        // And flush all writes.
        unsafe
        {
            NativeMethods.XLogFlush(NativeMethods.GetXLogWriteRecPtr());
        }

        PgBackgroundWorker.RunTransaction(() => SyncWorkerCleanup(sub));

        // In case there is apply process running, it might be waiting for the table status change so tell it to
        // check.
        WorfWorker me = Worker.MyWorker;
        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
        {
            int apply = guard.Read((in _, workers) =>
                Worker.ApplyFind(workers, me.Dboid, me.Worker.Sync.Apply.SubId));
            if (apply >= 0 && guard[apply].Proc != 0)
            {
                Worker.SetProcLatch(guard[apply].Proc);
            }
        }

        PgLog.Log($"finished sync of table {nspname}.{relname} for subscriber {sub.Name}");
    }

    /// <summary>
    /// Create subscription sync status record in catalog.
    /// </summary>
    /// <param name="sync">The status.</param>
    internal static unsafe void CreateLocalSyncStatus(WorfSyncStatus sync)
    {
        RelationData* rel = OpenSyncStatus();
        TupleDescData* tupDesc = rel->rd_att;

        // Form a tuple.
        ulong* values = stackalloc ulong[NattsLocalSyncState];
        bool* nulls = stackalloc bool[NattsLocalSyncState];
        new Span<bool>(nulls, NattsLocalSyncState).Clear();

        values[AnumSyncKind - 1] = (byte)sync.Kind;
        values[AnumSyncSubid - 1] = sync.Subid;

        if (sync.Nspname.Length != 0)
        {
            values[AnumSyncNspname - 1] = (ulong)Worf.NameIn(Worf.PStrDup(sync.Nspname));
        }
        else
        {
            nulls[AnumSyncNspname - 1] = true;
        }

        if (sync.Relname.Length != 0)
        {
            values[AnumSyncRelname - 1] = (ulong)Worf.NameIn(Worf.PStrDup(sync.Relname));
        }
        else
        {
            nulls[AnumSyncRelname - 1] = true;
        }

        values[AnumSyncStatus - 1] = (byte)sync.Status;
        values[AnumSyncStatuslsn - 1] = sync.Statuslsn;

        HeapTupleData* tup = NativeMethods.heap_form_tuple(tupDesc, values, nulls);

        // Insert the tuple to the catalog.
        NativeMethods.CatalogTupleInsert(rel, tup);

        // Cleanup.
        NativeMethods.heap_freetuple(tup);
        NativeMethods.table_close(rel, RowExclusiveLock);
    }

    /// <summary>
    /// Remove subscription sync status record from catalog.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    internal static unsafe void DropSubscriptionSyncStatus(uint subid)
    {
        RelationData* rel = OpenSyncStatus();

        ScanKeyData key;
        NativeMethods.ScanKeyInit(&key, AnumSyncSubid, BtEqualStrategyNumber, FOideq, subid);

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 1, &key);

        // Remove the tuples.
        HeapTupleData* tuple;
        while ((tuple = NativeMethods.systable_getnext(scan)) is not null)
        {
            NativeMethods.simple_heap_delete(rel, &tuple->t_self);
        }

        // Cleanup.
        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);
    }

    /// <summary>
    /// Get the sync status for a subscription.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    /// <param name="missingOk">Whether to return null when there's none.</param>
    /// <returns>The status, or null.</returns>
    internal static unsafe WorfSyncStatus? GetSubscriptionSyncStatus(uint subid, bool missingOk)
    {
        RelationData* rel = OpenSyncStatus();
        TupleDescData* tupDesc = rel->rd_att;

        ScanKeyData key;
        NativeMethods.ScanKeyInit(&key, AnumSyncSubid, BtEqualStrategyNumber, FOideq, subid);

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 1, &key);
        HeapTupleData* tuple = FindSubscriptionStatus(scan, tupDesc);

        if (tuple is null)
        {
            if (missingOk)
            {
                NativeMethods.systable_endscan(scan);
                NativeMethods.table_close(rel, RowExclusiveLock);
                return null;
            }

            throw new PgException(PgSqlStates.InternalError, $"subscription {subid} status not found");
        }

        WorfSyncStatus sync = SyncStatusFromTuple(tuple, tupDesc);

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);

        return sync;
    }

    /// <summary>
    /// Set the sync status for a subscription.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    /// <param name="status">The status.</param>
    internal static unsafe void SetSubscriptionSyncStatus(uint subid, char status)
    {
        RelationData* rel = OpenSyncStatus();
        TupleDescData* tupDesc = rel->rd_att;

        ScanKeyData key;
        NativeMethods.ScanKeyInit(&key, AnumSyncSubid, BtEqualStrategyNumber, FOideq, subid);

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 1, &key);
        HeapTupleData* oldtup = FindSubscriptionStatus(scan, tupDesc);

        if (oldtup is null)
        {
            throw new PgException(PgSqlStates.InternalError, $"subscription {subid} status not found");
        }

        UpdateSyncStatus(rel, oldtup, status, 0);

        // Cleanup.
        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);
    }

    /// <summary>
    /// Remove table sync status record from catalog.
    /// </summary>
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    internal static unsafe void DropTableSyncStatus(string nspname, string relname)
    {
        RelationData* rel = OpenSyncStatus();

        ScanKeyData* key = stackalloc ScanKeyData[2];
        NativeMethods.ScanKeyInit(&key[0], AnumSyncNspname, BtEqualStrategyNumber, FNameeq, (ulong)Worf.PStrDup(nspname));
        NativeMethods.ScanKeyInit(&key[1], AnumSyncRelname, BtEqualStrategyNumber, FNameeq, (ulong)Worf.PStrDup(relname));

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 2, key);

        // Remove the tuples.
        HeapTupleData* tuple;
        while ((tuple = NativeMethods.systable_getnext(scan)) is not null)
        {
            NativeMethods.simple_heap_delete(rel, &tuple->t_self);
        }

        // Cleanup.
        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);
    }

    /// <summary>
    /// Remove table sync status record from catalog.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    internal static unsafe void DropTableSyncStatusForSub(uint subid, string nspname, string relname)
    {
        RelationData* rel = OpenSyncStatus();

        ScanKeyData* key = stackalloc ScanKeyData[3];
        InitTableKeys(key, subid, nspname, relname);

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 3, key);

        // Remove the tuples.
        HeapTupleData* tuple;
        while ((tuple = NativeMethods.systable_getnext(scan)) is not null)
        {
            NativeMethods.simple_heap_delete(rel, &tuple->t_self);
        }

        // Cleanup.
        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);
    }

    /// <summary>
    /// Get the sync status for a table.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    /// <param name="missingOk">Whether to return null when there's none.</param>
    /// <returns>The status, or null.</returns>
    internal static unsafe WorfSyncStatus? GetTableSyncStatus(uint subid, string nspname, string relname, bool missingOk)
    {
        RelationData* rel = OpenSyncStatus();

        // Find an index we can use to scan this catalog.
        uint idxoid = 0;
        List* indexes = NativeMethods.RelationGetIndexList(rel);
        for (int i = 0; indexes is not null && i < indexes->length; i++)
        {
            uint indexoid = indexes->elements[i].oid_value;
            RelationData* idx = NativeMethods.index_open(indexoid, AccessShareLock);
            short* indkey = (short*)(&idx->rd_index->indkey + 1);
            bool matches = idx->rd_index->indnatts >= 3 &&
                indkey[0] == AnumSyncSubid && indkey[1] == AnumSyncNspname && indkey[2] == AnumSyncRelname;
            NativeMethods.index_close(idx, AccessShareLock);
            if (matches)
            {
                idxoid = indexoid;
                break;
            }
        }

        if (idxoid == 0)
        {
            throw new PgException(PgSqlStates.InternalError, "could not find index on local_sync_status");
        }

        NativeMethods.list_free(indexes);

        TupleDescData* tupDesc = rel->rd_att;

        ScanKeyData* key = stackalloc ScanKeyData[3];
        InitTableKeys(key, subid, nspname, relname);

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, idxoid, true, null, 3, key);
        HeapTupleData* tuple = NativeMethods.systable_getnext(scan);

        if (tuple is null)
        {
            if (missingOk)
            {
                NativeMethods.systable_endscan(scan);
                NativeMethods.table_close(rel, RowExclusiveLock);
                return null;
            }

            throw new PgException(PgSqlStates.InternalError, $"subscription {subid} table {nspname}.{relname} status not found");
        }

        WorfSyncStatus sync = SyncStatusFromTuple(tuple, tupDesc);

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);

        return sync;
    }

    /// <summary>
    /// Get the sync status for the subscription's tables that are not ready.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    /// <returns>The statuses.</returns>
    internal static List<WorfSyncStatus> GetUnsyncedTables(uint subid)
        => GetSubscriptionTables(subid).FindAll(static sync => sync.Status != SyncStatusReady);

    /// <summary>
    /// Get the sync status for all tables known to subscription.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    /// <returns>The statuses.</returns>
    internal static unsafe List<WorfSyncStatus> GetSubscriptionTables(uint subid)
    {
        List<WorfSyncStatus> res = [];

        RelationData* rel = OpenSyncStatus();
        TupleDescData* tupDesc = rel->rd_att;

        ScanKeyData key;
        NativeMethods.ScanKeyInit(&key, AnumSyncSubid, BtEqualStrategyNumber, FOideq, subid);

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 1, &key);
        HeapTupleData* tuple;
        while ((tuple = NativeMethods.systable_getnext(scan)) is not null)
        {
            if (IsSubscriptionStatus(tuple, tupDesc))
            {
                continue;
            }

            res.Add(SyncStatusFromTuple(tuple, tupDesc));
        }

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);

        return res;
    }

    /// <summary>
    /// Set the sync status for a table.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    /// <param name="status">The status.</param>
    /// <param name="statuslsn">The position the status was reached at.</param>
    internal static unsafe void SetTableSyncStatus(uint subid, string nspname, string relname, char status, ulong statuslsn)
    {
        RelationData* rel = OpenSyncStatus();

        ScanKeyData* key = stackalloc ScanKeyData[3];
        InitTableKeys(key, subid, nspname, relname);

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 3, key);
        HeapTupleData* oldtup = NativeMethods.systable_getnext(scan);

        if (oldtup is null)
        {
            throw new PgException(PgSqlStates.InternalError, $"subscription {subid} table {nspname}.{relname} status not found");
        }

        UpdateSyncStatus(rel, oldtup, status, statuslsn);

        // Cleanup.
        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);
    }

    /// <summary>
    /// Wait until the table sync status has changed desired one. We also exit if the worker is no longer recognized
    /// as sync worker as that means something bad happened to it.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    /// <param name="desiredState">The status to wait for.</param>
    /// <param name="lsn">The position the status was reached at.</param>
    /// <param name="keepalive">Called every second while waiting, or null to wait as long as the C does.</param>
    /// <returns>Whether the table reached the status.</returns>
    internal static bool WaitForSyncStatusChange(uint subid, string nspname, string relname, char desiredState, out ulong lsn,
        Action? keepalive = null)
    {
        bool ret = false;
        ulong statuslsn = 0;
        uint myDatabaseId;
        unsafe
        {
            myDatabaseId = NativeGlobals.MyDatabaseId;
        }

        while (!Worker.GotSigterm)
        {
            int result = PgBackgroundWorker.RunTransaction(() =>
            {
                WorfSyncStatus? sync = GetTableSyncStatus(subid, nspname, relname, true);
                if (sync is null)
                {
                    return 0;
                }

                if (sync.Status == desiredState)
                {
                    statuslsn = sync.Statuslsn;
                    return 1;
                }

                return -1;
            });

            if (result == 0)
            {
                break;
            }

            if (result == 1)
            {
                ret = true;
                break;
            }

            // Check if the worker is still alive - no point waiting if it died.
            int worker;
            byte[] serverNspname = Worf.ToServer(nspname);
            byte[] serverRelname = Worf.ToServer(relname);
            using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
            {
                worker = guard.Read((in _, workers) =>
                    Worker.SyncFind(workers, myDatabaseId, subid, serverNspname, serverRelname));
            }

            if (worker < 0)
            {
                break;
            }

            keepalive?.Invoke();

            // Wait for table sync progress. On Windows, use a shorter interval for faster detection of worker
            // failures.
            int timeout = keepalive is not null ? 1000 : OperatingSystem.IsWindows() ? 10000 : 60000;
            if (!PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(timeout)) &&
                !PgBackgroundWorker.CanContinue)
            {
                break;
            }
        }

        lsn = statuslsn;
        return ret;
    }

    /// <summary>
    /// Truncates table if it exists.
    /// </summary>
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    internal static unsafe void TruncateTable(string nspname, string relname)
    {
        RangeVar* rv = NativeMethods.makeRangeVar(Worf.PStrDup(nspname), Worf.PStrDup(relname), -1);

        uint relid = NativeMethods.RangeVarGetRelidExtended(rv, (int)PgLockMode.AccessExclusive, 1 /* RVR_MISSING_OK */, default, null);
        if (relid == 0)
        {
            return;
        }

        // Truncate the table.
        TruncateStmt* truncate = (TruncateStmt*)NativeMethods.palloc0((ulong)sizeof(TruncateStmt));
        truncate->type = NodeTag.T_TruncateStmt;
        truncate->relations = NativeMethods.lappend(null, rv);
        truncate->restart_seqs = false;
        truncate->behavior = DropBehavior.DROP_RESTRICT;

        NativeMethods.ExecuteTruncate(truncate);

        NativeMethods.CommandCounterIncrement();
    }

    /// <summary>
    /// Run a command and wait for it to exit, then return its exit code.
    /// </summary>
    private static int ExecCmd(string cmd, List<string> cmdargv) => PgChildProcess.Run(cmd, CollectionsMarshal.AsSpan(cmdargv));

    private static string GetPgExecutable(string cmdname)
    {
        string myExecPath;
        unsafe
        {
            myExecPath = Worf.FromServer((sbyte*)NativeGlobals.DangerousAddressOf_my_exec_path());
        }

        if (Fe.FindOtherExecVersion(myExecPath, cmdname, out uint version, out string cmdbuf) != 0)
        {
            throw new PgException(PgSqlStates.InternalError,
                $"worf subscriber init failed to find {cmdname} relative to binary {myExecPath}");
        }

        int pgVersionNum = Worf.ServerVersionNum;
        if (version / 100 != pgVersionNum / 100)
        {
            throw new PgException(PgSqlStates.InternalError,
                $"worf subscriber init found {cmdname} with wrong major version {version / 100 / 100}.{version / 100 % 100}, " +
                $"expected {pgVersionNum / 100 / 100}.{pgVersionNum / 100 % 100}");
        }

        return cmdbuf;
    }

    private static void DumpStructure(WorfSubscription sub, string destfile, string snapshot)
    {
        string dsn = Fe.GetConnstr(sub.OriginIf.Dsn, null, null, out string? errMsg)
            ?? throw new PgException(PgSqlStates.InternalError, $"invalid connection string \"{sub.OriginIf.Dsn}\": {errMsg}");

        string pgDump = GetPgExecutable(PgdumpBinary);

        List<string> cmdargv =
        [
            // custom format
            "-Fc",

            // schema only
            "-s",

            // snapshot
            $"--snapshot={snapshot}",

            // Dumping database, filter out our extension.
            $"--exclude-schema={Worf.ExtensionName}",
        ];

        // Skip the worf_origin if it exists locally.
        bool hasPglOrigin = PgBackgroundWorker.RunTransaction(() =>
            Worf.ReadScalar<bool>("SELECT EXISTS (SELECT FROM pg_catalog.pg_namespace WHERE nspname = 'worf_origin')"));
        if (hasPglOrigin)
        {
            cmdargv.Add($"--exclude-schema={"worf_origin"}");
        }

        // destination file
        cmdargv.Add($"--file={destfile}");

        // connection string
        cmdargv.Add($"--dbname={dsn}");

        if (ExecCmd(pgDump, cmdargv) != 0)
        {
            throw new PgException(PgSqlStates.InternalError, $"could not execute pg_dump (\"{pgDump}\"): {LastErrorMessage()}");
        }
    }

    private static void RestoreStructure(WorfSubscription sub, string srcfile, string? section)
    {
        string dsn = Fe.GetConnstr(sub.TargetIf.Dsn, null, "-cworf.subscription_schema_restore=true", out string? errMsg)
            ?? throw new PgException(PgSqlStates.InternalError, $"invalid connection string \"{sub.TargetIf.Dsn}\": {errMsg}");

        string pgRestore = GetPgExecutable(PgrestoreBinary);

        List<string> cmdargv = [];

        // section
        if (section is not null)
        {
            cmdargv.Add($"--section={section}");
        }

        // stop execution on any error
        cmdargv.Add("--exit-on-error");

        // apply everything in single tx
        cmdargv.Add("-1");

        // connection string
        cmdargv.Add($"--dbname={dsn}");

        // source file
        cmdargv.Add(srcfile);

        if (ExecCmd(pgRestore, cmdargv) != 0)
        {
            throw new PgException(PgSqlStates.InternalError, $"could not execute pg_restore (\"{pgRestore}\"): {LastErrorMessage()}");
        }
    }

    /// <summary>
    /// Creates the subscription's slot and gets its exported snapshot, using a SQL connection of its own that is closed
    /// once the slot exists.
    /// </summary>
    private static string EnsureReplicationSlotSnapshot(WorfSubscription sub, PgReplicationConnection replConn, string appName,
        bool useFailoverSlot, out ulong lsn)
    {
        using PgReplicationConnection sqlConn = Worf.Connect(sub.OriginIf.Dsn, sub.Name, appName);
        ulong slotLsn = 0;
        string snapshot = PgBackgroundWorker.RunTransaction(() =>
            EnsureReplicationSlotSnapshot(sqlConn, replConn, sub.SlotName, useFailoverSlot, out slotLsn));
        lsn = slotLsn;
        return snapshot;
    }

    /// <summary>
    /// Create slot and get the exported snapshot. This will try to recreate slot if already exists and not active.
    /// The reported LSN is the confirmed flush LSN at the point the slot reached consistency and exported its
    /// snapshot.
    /// </summary>
    private static string EnsureReplicationSlotSnapshot(PgReplicationConnection sqlConn, PgReplicationConnection replConn,
        string slotName, bool useFailoverSlot, out ulong lsn)
    {
        while (true)
        {
            string query = $"CREATE_REPLICATION_SLOT \"{slotName}\" LOGICAL {Worf.ExtensionName}{(useFailoverSlot ? " FAILOVER" : "")}";

            IReadOnlyList<string?[]> res;
            try
            {
                res = replConn.Query(query, 4);
            }
            catch (PgRemoteException error)
            {
                // If our slot already exist but is not used, it's leftover from previous unsucessful attempt to
                // synchronize table, try dropping it and recreating.
                if (error.SqlState == "42710" && !Rpc.RemoteSlotActive(sqlConn, slotName))
                {
                    Rpc.DropRemoteSlot(sqlConn, slotName);
                    continue;
                }

                throw new PgException(PgSqlStates.InternalError, $"could not create replication slot on provider: ERROR:  {error.Message}\n\n");
            }

            if (!Worf.TryParseLsn(res[0][1] ?? "", out lsn))
            {
                throw new PgException(PgSqlStates.InvalidTextRepresentation,
                    $"invalid input syntax for type pg_lsn: \"{res[0][1]}\"");
            }

            return res[0][2] ?? "";
        }
    }

    /// <summary>
    /// Get or create replication origin for a given slot.
    /// </summary>
    private static ushort EnsureReplicationOrigin(WorfSubscription sub)
        => PgReplicationOrigin.Find(sub.SlotName) ?? WorfNode.CreateOrigin(sub.SlotName, sub.Origin.Id);

    /// <summary>
    /// Transaction management for COPY.
    /// </summary>
    private static void StartCopyOriginTx(PgReplicationConnection conn, string? snapshot)
    {
        List<string> setupQuery =
        [
            "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
            "SET DATESTYLE = ISO",
            "SET INTERVALSTYLE = POSTGRES",
            "SET extra_float_digits TO 3",
            "SET statement_timeout = 0",
            "SET lock_timeout = 0",
        ];

        if (snapshot is not null)
        {
            setupQuery.Add($"SET TRANSACTION SNAPSHOT {Spi.QuoteLiteral(snapshot)}");
        }

        try
        {
            foreach (string command in setupQuery)
            {
                conn.Execute(command);
            }
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, $"BEGIN on origin node failed: ERROR:  {error.Message}\n");
        }
    }

    /// <summary>
    /// Starts the local transaction the copied rows go into, as the target connection's transaction.
    /// </summary>
    private static void StartCopyTargetTx(string originName)
    {
        // Set correct origin. We must do this before starting the copy.
        ushort origin = PgReplicationOrigin.Find(originName)
            ?? throw new PgException(PgSqlStates.UndefinedObject, $"replication origin \"{originName}\" does not exist");
        PgReplicationOrigin.SetupSession(origin);

        Spi.Execute("SET LOCAL session_replication_role = 'replica'");
        Spi.Execute("SET LOCAL DATESTYLE = ISO");
        Spi.Execute("SET LOCAL INTERVALSTYLE = POSTGRES");
        Spi.Execute("SET LOCAL extra_float_digits TO 3");
        Spi.Execute("SET LOCAL statement_timeout = 0");
        Spi.Execute("SET LOCAL lock_timeout = 0");
    }

    private static void FinishCopyOriginTx(PgReplicationConnection conn)
    {
        // Close the transaction and connection on origin node.
        try
        {
            conn.Execute("ROLLBACK");
        }
        catch (PgRemoteException error)
        {
            PgLog.Warning($"ROLLBACK on origin node failed: ERROR:  {error.Message}\n");
        }

        conn.Dispose();
    }

    private static void FinishCopyTargetTx()
    {
        // Resetting the origin explicitly before the transaction's process goes on will help prevent races with
        // other accesses to the same replication origin.
        if (PgReplicationOrigin.SessionOrigin != 0)
        {
            PgReplicationOrigin.ResetSession();
        }
    }

    private static int PhysattInAttmap(WorfRelation rel, int attid)
    {
        for (int i = 0; i < rel.Natts; i++)
        {
            if (rel.Attmap[i] == attid)
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Create list of columns for COPY based on logical relation mapping.
    /// </summary>
    private static unsafe List<string> MakeCopyAttnamelist(WorfRelation rel)
    {
        List<string> attnamelist = [];
        TupleDescData* desc = rel.Rel->rd_att;

        for (int attnum = 0; attnum < desc->natts; attnum++)
        {
            int remoteattnum = PhysattInAttmap(rel, attnum);

            // Skip dropped attributes.
            if (NativeMethods.TupleDescAttr(desc, attnum)->attisdropped)
            {
                continue;
            }

            if (remoteattnum < 0)
            {
                continue;
            }

            attnamelist.Add(rel.Attnames[remoteattnum]);
        }

        return attnamelist;
    }

    /// <summary>
    /// COPY single table over wire. Runs in the target transaction.
    /// </summary>
    private static void CopyTableData(PgReplicationConnection originConn, WorfRemoteRel remoterel, List<string> replicationSets)
    {
        // Build the relation map.
        RelCache.RelationCacheUpdater(remoterel);
        WorfRelation rel = RelCache.RelationOpen(remoterel.Relid, PgLockMode.AccessShare);
        List<string> attnamelist = MakeCopyAttnamelist(rel);
        uint reloid = rel.Reloid;

        StringBuilder attlist = new();
        bool first = true;
        foreach (string attname in attnamelist)
        {
            if (first)
            {
                first = false;
            }
            else
            {
                attlist.Append(',');
            }

            attlist.Append(Spi.QuoteIdentifier(attname));
        }

        RelCache.RelationClose(rel, PgLockMode.AccessShare);

        // Build COPY TO query.
        StringBuilder query = new("COPY ");

        // If the table is row-filtered we need to run query over the table to execute the filter.
        if (remoterel.HasRowFilter)
        {
            string relname = $"{Spi.QuoteIdentifier(remoterel.Nspname)}.{Spi.QuoteIdentifier(remoterel.Relname)}";

            StringBuilder repsetarr = new();
            first = true;
            foreach (string repsetName in replicationSets)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    repsetarr.Append(',');
                }

                repsetarr.Append(Spi.QuoteLiteral(repsetName));
            }

            query.Append(CultureInfo.InvariantCulture, $"(SELECT {(attnamelist.Count != 0 ? attlist.ToString() : "*")} FROM worf.table_data_filtered(NULL::{relname}, {Spi.QuoteLiteral(relname)}::regclass, ARRAY[{repsetarr}])) ");
        }
        else
        {
            // Otherwise just copy the table.
            query.Append(CultureInfo.InvariantCulture, $"{Spi.QuoteIdentifier(remoterel.Nspname)}.{Spi.QuoteIdentifier(remoterel.Relname)} ");

            if (attnamelist.Count != 0)
            {
                query.Append(CultureInfo.InvariantCulture, $"({attlist}) ");
            }
        }

        query.Append("TO stdout");

        // Execute COPY TO.
        try
        {
            originConn.StartCopy(query.ToString());
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, "table copy failed",
                detail: $"Query '{query}': ERROR:  {error.Message}\n");
        }

        // Execute COPY FROM.
        _ = originConn.CopyInto(reloid, attnamelist.Count != 0 ? attnamelist : null);

        PgLog.Info($"finished synchronization of data for table {remoterel.Nspname}.{remoterel.Relname}");
    }

    /// <summary>
    /// Copy data from origin node to target node. Creates new connection to origin.
    /// </summary>
    private static void CopyTablesData(string subName, string originDsn, string originSnapshot,
        List<(string Nspname, string Relname)> tables, List<string> replicationSets, string originName)
    {
        // Connect to origin node.
        PgReplicationConnection originConn = Worf.Connect(originDsn, subName, "copy");
        StartCopyOriginTx(originConn, originSnapshot);

        // Copy every table in the target transaction.
        try
        {
            PgBackgroundWorker.RunTransaction(() =>
            {
                StartCopyTargetTx(originName);
                foreach ((string nspname, string relname) in tables)
                {
                    WorfRemoteRel remoterel = Rpc.GetRemoteRepsetTable(originConn, nspname, relname, replicationSets);

                    CopyTableData(originConn, remoterel, replicationSets);

                    PgInterrupts.Check();
                }
            });
        }
        finally
        {
            FinishCopyTargetTx();
        }

        // Finish the transactions and disconnect.
        FinishCopyOriginTx(originConn);
    }

    /// <summary>
    /// Copy data from origin node to target node. Creates new connection to origin. This is basically same as the
    /// <see cref="CopyTablesData"/>, but it can't be easily merged to single function because we need to get list of
    /// tables here after the transaction is bound to a snapshot.
    /// </summary>
    private static List<WorfRemoteRel> CopyReplicationSetsData(string subName, string originDsn, string originSnapshot,
        List<string> replicationSets, string originName)
    {
        // Connect to origin node.
        PgReplicationConnection originConn = Worf.Connect(originDsn, subName, "copy");
        StartCopyOriginTx(originConn, originSnapshot);

        // Get tables to copy from origin node, and copy every table in the target transaction.
        List<WorfRemoteRel> tables;
        try
        {
            tables = PgBackgroundWorker.RunTransaction(() =>
            {
                List<WorfRemoteRel> tables = Rpc.GetRemoteRepsetTables(originConn, replicationSets);

                StartCopyTargetTx(originName);
                foreach (WorfRemoteRel remoterel in tables)
                {
                    CopyTableData(originConn, remoterel, replicationSets);

                    PgInterrupts.Check();
                }

                return tables;
            });
        }
        finally
        {
            FinishCopyTargetTx();
        }

        // Finish the transactions and disconnect.
        FinishCopyOriginTx(originConn);

        return tables;
    }

    private static void SyncWorkerCleanup(WorfSubscription sub)
    {
        // Drop the slot on the remote side.
        using (PgReplicationConnection originConn = Worf.Connect(sub.OriginIf.Dsn, sub.Name, "cleanup"))
        {
            // Wait for slot to be free.
            while (!Worker.GotSigterm)
            {
                if (!Rpc.RemoteSlotActive(originConn, sub.SlotName))
                {
                    break;
                }

                // Wait for remote slot to become inactive. On Windows, use a shorter interval for faster response
                // times.
                _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(OperatingSystem.IsWindows() ? 200 : 1000));
            }

            Rpc.DropRemoteSlot(originConn, sub.SlotName);
        }

        // Drop the origin tracking locally.
        if (PgReplicationOrigin.SessionOrigin != 0)
        {
            PgReplicationOrigin.ResetSession();
            WorfNode.DropOrigins(sub.SlotName);
        }
    }

    /// <summary>
    /// Cleans the subscription's slot up after an error, as the error cleanup callback does, and lets the error
    /// propagate.
    /// </summary>
    private static bool CleanupAfterError(WorfSubscription sub)
    {
        try
        {
            PgBackgroundWorker.RunTransaction(() => SyncWorkerCleanup(sub));
        }
        catch (PgException error)
        {
            PgLog.Warning($"could not clean up after the failed synchronization: {error.Message}");
        }

        return false;
    }

    private static void SyncTmpfileCleanup(string tmpfile)
    {
        try
        {
            File.Delete(tmpfile);
        }
        catch (IOException error)
        {
            PgLog.Warning($"Failed to clean up worf temporary dump file \"{tmpfile}\" on exit/error: {error.Message}");
        }
        catch (UnauthorizedAccessException error)
        {
            PgLog.Warning($"Failed to clean up worf temporary dump file \"{tmpfile}\" on exit/error: {error.Message}");
        }
    }

    private static unsafe WorfSyncStatus SyncStatusFromTuple(HeapTupleData* tuple, TupleDescData* desc)
    {
        bool isnull;
        WorfSyncStatus sync = new()
        {
            Kind = (char)(byte)NativeMethods.heap_getattr(tuple, AnumSyncKind, desc, &isnull),
            Subid = (uint)NativeMethods.heap_getattr(tuple, AnumSyncSubid, desc, &isnull),
        };

        ulong d = NativeMethods.heap_getattr(tuple, AnumSyncNspname, desc, &isnull);
        if (!isnull)
        {
            sync.Nspname = Worf.FromServer((sbyte*)d);
        }

        d = NativeMethods.heap_getattr(tuple, AnumSyncRelname, desc, &isnull);
        if (!isnull)
        {
            sync.Relname = Worf.FromServer((sbyte*)d);
        }

        sync.Status = (char)(byte)NativeMethods.heap_getattr(tuple, AnumSyncStatus, desc, &isnull);
        sync.Statuslsn = NativeMethods.heap_getattr(tuple, AnumSyncStatuslsn, desc, &isnull);
        return sync;
    }

    private static unsafe RelationData* OpenSyncStatus() => Worf.OpenCatalog(CatalogLocalSyncStatus, RowExclusiveLock);

    // The subscription's own status is the row with no table.
    private static unsafe bool IsSubscriptionStatus(HeapTupleData* tuple, TupleDescData* desc)
    {
        bool nspnull;
        bool relnull;
        _ = NativeMethods.heap_getattr(tuple, AnumSyncNspname, desc, &nspnull);
        _ = NativeMethods.heap_getattr(tuple, AnumSyncRelname, desc, &relnull);
        return nspnull && relnull;
    }

    private static unsafe HeapTupleData* FindSubscriptionStatus(SysScanDescData* scan, TupleDescData* desc)
    {
        HeapTupleData* tuple;
        while ((tuple = NativeMethods.systable_getnext(scan)) is not null)
        {
            if (IsSubscriptionStatus(tuple, desc))
            {
                break;
            }
        }

        return tuple;
    }

    private static unsafe void InitTableKeys(ScanKeyData* key, uint subid, string nspname, string relname)
    {
        NativeMethods.ScanKeyInit(&key[0], AnumSyncSubid, BtEqualStrategyNumber, FOideq, subid);
        NativeMethods.ScanKeyInit(&key[1], AnumSyncNspname, BtEqualStrategyNumber, FNameeq, (ulong)Worf.PStrDup(nspname));
        NativeMethods.ScanKeyInit(&key[2], AnumSyncRelname, BtEqualStrategyNumber, FNameeq, (ulong)Worf.PStrDup(relname));
    }

    private static unsafe void UpdateSyncStatus(RelationData* rel, HeapTupleData* oldtup, char status, ulong statuslsn)
    {
        ulong* values = stackalloc ulong[NattsLocalSyncState];
        bool* nulls = stackalloc bool[NattsLocalSyncState];
        bool* replaces = stackalloc bool[NattsLocalSyncState];
        new Span<bool>(nulls, NattsLocalSyncState).Clear();
        new Span<bool>(replaces, NattsLocalSyncState).Clear();

        values[AnumSyncStatus - 1] = (byte)status;
        replaces[AnumSyncStatus - 1] = true;
        values[AnumSyncStatuslsn - 1] = statuslsn;
        replaces[AnumSyncStatuslsn - 1] = true;

        HeapTupleData* newtup = NativeMethods.heap_modify_tuple(oldtup, rel->rd_att, values, nulls, replaces);

        // Update the tuple in catalog.
        NativeMethods.CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

        NativeMethods.heap_freetuple(newtup);
    }

    private const string CatalogLocalSyncStatus = "local_sync_status";

    private const int NattsLocalSyncState = 6;
    private const short AnumSyncKind = 1;
    private const short AnumSyncSubid = 2;
    private const short AnumSyncNspname = 3;
    private const short AnumSyncRelname = 4;
    private const short AnumSyncStatus = 5;
    private const short AnumSyncStatuslsn = 6;

    private const int AccessShareLock = 1;
    private const int RowExclusiveLock = 3;
    private const ushort BtEqualStrategyNumber = 3;
    private const uint FOideq = 184;
    private const uint FNameeq = 62;

    private static string LastErrorMessage()
        => new Win32Exception(Marshal.GetLastSystemError()).Message;
}
