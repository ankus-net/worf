using System.ComponentModel;
using System.Diagnostics;
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

    CREATE TABLE worf.sync_slot (
        sub_id oid NOT NULL,
        slot_name name NOT NULL,
        PRIMARY KEY (sub_id, slot_name)
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

    // How many seconds a table sync worker waits for its slot, held by another process, before it fails to catch up.
    private const int CatchupSlotWait = 60;

    // How many seconds a table sync worker's cleanup waits for its slot, held by another process, before it leaves the
    // slot to the apply worker.
    private const int CleanupSlotWait = 5;

    // The suffix of the application name of a table sync worker's connection that catches up.
    private const string CatchupConnection = "catchup";

    // How long apply may get no further before a table's copy first lets its snapshot go, and how long a copy waits for
    // apply before saying so, and then how often.
    private static TimeSpan ApplyStallTimeout => TimeSpan.FromSeconds(10);
    private static TimeSpan ApplyWaitLogAfter => TimeSpan.FromSeconds(10);
    private static TimeSpan ApplyWaitLogInterval => TimeSpan.FromSeconds(60);

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

            // We can recover from crashes during these. A data copy that failed rolled back, and starts again.
            case SyncStatusInit:
            case SyncStatusData:
            case SyncStatusCatchup:
                break;

            default:
                throw new PgException(PgSqlStates.InternalError,
                    $"subscriber {sub.Name} initialization failed during nonrecoverable step ({status}), please try the setup again");
        }

        if (status is SyncStatusInit or SyncStatusData)
        {
            const bool useFailoverSlot = false;

            WorfLog.Info($"initializing subscriber {sub.Name}");

            // The provider's managed sequences are joined before any rows arrive.
            List<WorfManagedSequence> managedSequences = PgBackgroundWorker.RunTransaction(() => SequenceRangeSync.FetchManaged(sub.OriginIf.Dsn, sub.Name));

            using WorfConnection originConnRepl = Worf.ConnectReplica(sub.OriginIf.Dsn, sub.Name, "snap");

            string snapshot = EnsureReplicationSlotSnapshot(sub, originConnRepl, "snap", useFailoverSlot, out lsn);

            try
            {
                string tmpfile = Path.GetFullPath(Path.Join(Worf.TempDirectory, $"worf-{Environment.ProcessId}.dump"));

                try
                {
                    Apply.RunTakingOriginSlot(() =>
                    {
                        ushort originid = EnsureReplicationOrigin(sub);
                        ulong xactLastCommitEnd;
                        unsafe
                        {
                            xactLastCommitEnd = NativeGlobals.XactLastCommitEnd;
                        }

                        WorfLog.Debug3($"advancing origin with oid {originid} for forwarded row to {Worf.FormatLsn(xactLastCommitEnd)} during subscription sync");
                        PgReplicationOrigin.Advance(originid, new PgLsn(lsn), new PgLsn(xactLastCommitEnd), true, true);
                    });

                    // A data copy that starts again finds the structure restored, and dumps it for its constraints.
                    if (SyncKindStructureCopy(sync.Kind))
                    {
                        if (status == SyncStatusInit)
                        {
                            WorfLog.Info("synchronizing structure");

                            PgBackgroundWorker.RunTransaction(() =>
                            {
                                SetSubscriptionSyncStatus(sub.Id, SyncStatusStructure);
                                SequenceRangeSync.RecordForRestore(managedSequences);
                            });
                        }

                        // Dump structure to temp storage.
                        DumpStructure(sub, tmpfile, snapshot);

                        // Restore base pre-data structure (types, tables, etc).
                        if (status == SyncStatusInit)
                        {
                            RestoreStructure(sub, tmpfile, "pre-data");
                        }
                    }

                    // Copy data.
                    if (SyncKindData(sync.Kind))
                    {
                        WorfLog.Info("synchronizing data");

                        PgBackgroundWorker.RunTransaction(() =>
                        {
                            SetSubscriptionSyncStatus(sub.Id, SyncStatusData);
                            SequenceRangeSync.JoinBeforeCopy(managedSequences, 0);
                        });

                        // The tables' statuses are stored, and the subscription's moves past the copy, as the copy
                        // commits, so that a copy that fails starts again.
                        CopyReplicationSetsData(sub.Name, sub.OriginIf.Dsn, snapshot, sub.ReplicationSets, sub.SlotName, lsn,
                            tables =>
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

                                SetSubscriptionSyncStatus(sub.Id, SyncKindStructureCopy(sync.Kind) ? SyncStatusConstraints : SyncStatusCatchup);
                            });
                    }

                    // Restore post-data structure (indexes, constraints, etc).
                    if (SyncKindStructureCopy(sync.Kind))
                    {
                        WorfLog.Info("synchronizing constraints");

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
            catch (PgException)
            {
                // A row that failed to convert under sub_disable disables the subscription, once the copy rolled back.
                // The next worker starts later each time the synchronization fails.
                CleanupAfterError(sub);
                CopyConversion.DisableAfterFailure();
                CountSyncFailures(true);
                throw;
            }
            catch (Exception)
            {
                CleanupAfterError(sub);
                throw;
            }

            CountSyncFailures(false);
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

        WorfLog.Info($"finished synchronization of subscriber {sub.Name}, ready to enter normal replication");
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

            // One that failed once its copy had committed, as while it caught up, may catch up again.
            if (sync.Status is SyncStatusSyncwait or SyncStatusCatchup)
            {
                return (sync.Status, sync.Statuslsn);
            }

            // If previous sync attempt failed, we need to start from beginning. Its copy committed nothing, as it
            // commits with the status that follows.
            if (sync.Status != SyncStatusInit)
            {
                SetTableSyncStatus(sub.Id, nspname, relname, SyncStatusInit, 0);
            }

            return (SyncStatusNone, sync.Statuslsn);
        });

        statusLsn = currentLsn;
        if (currentStatus is SyncStatusReady or SyncStatusSyncdone)
        {
            return currentStatus;
        }

        // The copy's rows, and the changes caught up after them, are kept. The slot and the origin's progress outlive
        // the worker, so it catches up from where it got to. Without them, the changes since can't be caught up, so the
        // table is copied again, and the copy merged into the rows it has, in one transaction.
        bool merge = false;
        if (currentStatus is SyncStatusSyncwait or SyncStatusCatchup)
        {
            if (CatchupStart(sub, out string? lost) is ulong start)
            {
                WorfLog.Log($"resuming the catch-up of table {nspname}.{relname} from {Worf.FormatLsn(start)}");
                statusLsn = start;
                return SyncStatusCatchup;
            }

            WorfLog.Warning(new PgDiagnostic($"copying table {nspname}.{relname} again, as its table sync can't catch up")
            {
                Detail = lost,
            });

            // The origin has no progress until the merge commits it, so that a worker that stops before then copies
            // again, rather than catching up through the new slot from where the first copy got to.
            Apply.RunTakingOriginSlot(() =>
                PgReplicationOrigin.Advance(EnsureReplicationOrigin(sub), PgLsn.Invalid, PgLsn.Invalid, true, true));
            merge = true;
            PgBackgroundWorker.RunTransaction(() => RequireMergeable(nspname, relname));
        }

        List<WorfManagedSequence> managedSequences = PgBackgroundWorker.RunTransaction(() => SequenceRangeSync.FetchManaged(sub.OriginIf.Dsn, sub.Name));
        using WorfConnection originConnRepl = Worf.ConnectReplica(sub.OriginIf.Dsn, sub.Name, "copy");

        // Make sure we cleanup the slot if something goes wrong.
        try
        {
            // The copy waits for apply to reach its snapshot, which keeps the provider from removing dead rows
            // meanwhile. So apply is waited for without one first, up to where the provider is now.
            ulong target = ProviderPosition(sub);
            long waitingSince = Stopwatch.GetTimestamp();
            TimeSpan stallTimeout = ApplyStallTimeout;
            string snapshot;
            ulong lsn;
            while (true)
            {
                _ = WaitForApply(sub, nspname, relname, target, TimeSpan.MaxValue, waitingSince);
                RecordSyncSlot(sub);
                snapshot = EnsureReplicationSlotSnapshot(sub, originConnRepl, "copy_slot", false, out lsn);
                if (WaitForApply(sub, nspname, relname, lsn, stallTimeout, waitingSince))
                {
                    break;
                }

                // Apply stopped short of the snapshot, as on a change that fails. Its slot is dropped to let the
                // snapshot go until apply gets there. Apply may get no further for twice as long each time after, so
                // that the copy starts in the end while changes keep coming that each take long to apply.
                WorfLog.Log($"dropping the snapshot for table {nspname}.{relname} until subscription {sub.Name} applies up to {Worf.FormatLsn(lsn)}, " +
                    $"since it got no further for {stallTimeout.TotalSeconds:0} s");
                PgBackgroundWorker.RunTransaction(() => originConnRepl.Execute($"DROP_REPLICATION_SLOT \"{sub.SlotName}\""));
                target = lsn;
                stallTimeout *= 2;
            }

            statusLsn = lsn;

            Apply.RunTakingOriginSlot(() =>
            {
                // The table's managed sequences are joined before any rows arrive.
                SequenceRangeSync.JoinBeforeCopy(managedSequences, Worf.ReadScalar<uint?>("SELECT to_regclass($1)::oid",
                    SpiParameter.Create(Worf.QuoteQualifiedIdentifier(nspname, relname))) ?? 0);

                ushort originid = EnsureReplicationOrigin(sub);
                if (merge)
                {
                    return;
                }

                ulong xactLastCommitEnd;
                unsafe
                {
                    xactLastCommitEnd = NativeGlobals.XactLastCommitEnd;
                }

                WorfLog.Debug2($"advancing origin {Apply.MySubscription!.SlotName} (oid {originid}) for forwarded row to {Worf.FormatLsn(xactLastCommitEnd)} after sync error");
                PgReplicationOrigin.Advance(originid, new PgLsn(lsn), new PgLsn(xactLastCommitEnd), true, true);

                SetTableSyncStatus(sub.Id, nspname, relname, SyncStatusData, lsn);
            });

            // Copy data. The status that says the copy has committed changes in the copy's transaction, so that a
            // worker that stops after it catches up rather than copying again. A merge's commit advances the origin too.
            CopyTablesData(sub.Name, sub.OriginIf.Dsn, snapshot, [(nspname, relname)], sub.ReplicationSets, sub.SlotName,
                lsn, merge, () => SetTableSyncStatus(sub.Id, nspname, relname, SyncStatusCatchup, lsn));
        }
        catch (PgException)
        {
            // A row that failed to convert under sub_disable disables the subscription, once the copy rolled back.
            CleanupAfterError(sub);
            CopyConversion.DisableAfterFailure();
            throw;
        }
        catch (Exception)
        {
            CleanupAfterError(sub);
            throw;
        }

        return SyncStatusCatchup;
    }

    /// <summary>
    /// Gets where a table sync worker whose copy has committed catches up from: the latest progress of its origin and
    /// relay origins, as apply starts from, when the slot it streams from is still on the provider and can stream.
    /// </summary>
    /// <param name="sub">The subscription, with the table's slot name.</param>
    /// <param name="lost">Why it can't catch up, when it can't.</param>
    /// <returns>The position, or null when the slot is gone or invalidated, or the origin is gone or has no progress.</returns>
    private static ulong? CatchupStart(WorfSubscription sub, out string? lost)
    {
        using (WorfConnection conn = Worf.Connect(sub.OriginIf.Dsn, sub.Name, "resume"))
        {
            lost = PgBackgroundWorker.RunTransaction(() => Rpc.RemoteSlotLost(conn, sub.SlotName));
            if (lost is not null)
            {
                return null;
            }
        }

        (ulong? start, lost) = PgBackgroundWorker.RunTransaction<(ulong?, string?)>(() =>
        {
            if (PgReplicationOrigin.Find(sub.SlotName) is not ushort origin)
            {
                return (null, $"Replication origin \"{sub.SlotName}\" does not exist.");
            }

            // The copy advanced the origin to the copy's position, so one with no progress was reset since.
            ulong progress = PgReplicationOrigin.GetProgress(origin, false).Value;
            if (progress == 0)
            {
                return (null, $"Replication origin \"{sub.SlotName}\" has no progress.");
            }

            foreach ((ushort relay, _) in WorfNode.RelayOrigins(sub.SlotName))
            {
                progress = Math.Max(progress, PgReplicationOrigin.GetProgress(relay, false).Value);
            }

            return (progress, null);
        });
        return start;
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
        string tablename = Worf.QuoteQualifiedIdentifier(nspname, relname);

        sub.SlotName = SyncSlotName(sub.SlotName, nspname, relname);

        WorfLog.Log($"starting sync of table {nspname}.{relname} for subscriber {sub.Name}");
        WorfLog.Debug1($"connecting to provider {sub.OriginIf.Name}, dsn {sub.OriginIf.Dsn}");

        // Do the initial sync first.
        char status = SyncTable(sub, nspname, relname, out ulong statusLsn);
        if (status is SyncStatusSyncdone or SyncStatusReady)
        {
            SyncWorkerFinish(nspname, relname);
            return;
        }

        // Ask the apply worker to stop for the catch-up, which it does by setting the status to catching up, with
        // where to stop.
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

        // A worker asked to stop before then exits, leaving the table to the next, which catches up from where this one
        // got to. Going on would finish at the copy's position, without the changes the apply worker skipped since.
        if (!WaitForSyncStatusChange(sub.Id, nspname, relname, SyncStatusCatchup, out _))
        {
            Worker.ProcExit(1);
        }

        // Setup the origin and get the starting position for the replication.
        bool done = PgBackgroundWorker.RunTransaction(() =>
        {
            ushort originid = PgReplicationOrigin.Find(sub.SlotName)
                ?? throw new PgException(PgSqlStates.UndefinedObject, $"replication origin \"{sub.SlotName}\" does not exist");
            WorfLog.Debug2($"setting origin {sub.SlotName} (oid {originid}) for subscription sync");
            PgReplicationOrigin.SetupSession(originid);
            Apply.LoadStreamOrigins();

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

        // Start the replication. To apply a failed transaction again under worf.exception_behaviour, it streams again
        // from that transaction.
        for (ulong startLsn = statusLsn; startLsn != 0;)
        {
            using WorfConnection streamConn = Worf.ConnectReplica(sub.OriginIf.Dsn, sub.Name, CatchupConnection);

            // IDENTIFY_SYSTEM sets up some internal state on walsender so call it even if we don't (yet) want to use
            // any of the results.
            _ = PgBackgroundWorker.RunTransaction(() => Worf.IdentifySystem(streamConn));

            // The walsender that streamed before holds the slot until it notices the connection closed, which it doesn't
            // while it's busy decoding, as with a large transaction it sends nothing of. Rather than fail and start
            // again, the worker waits for the slot, which is its own.
            WaitForCatchupSlot(streamConn, sub);

            Worf.StartReplication(streamConn, sub.SlotName, startLsn, "all", null, tablename, sub.ForceTextTransfer);

            // Leave it to standard apply code to do the replication.
            startLsn = Apply.ApplyWork(streamConn);
        }

        // We should only get here if we received sigTERM, which in case of sync worker is not expected.
        Worker.ProcExit(1);
    }

    /// <summary>
    /// Waits for a table sync worker's slot to be free before it streams from it. After a minute the worker fails, and
    /// catches up again when it restarts.
    /// </summary>
    /// <param name="streamConn">The replication connection the worker streams on.</param>
    /// <param name="sub">The subscription, with the table's slot name.</param>
    private static void WaitForCatchupSlot(WorfConnection streamConn, WorfSubscription sub)
    {
        int holder = WaitForSlot(streamConn, sub, "this table sync",
            $"The sync fails after {CatchupSlotWait} seconds, and catches up again when it restarts.", CatchupSlotWait);
        if (holder != 0 && !Worker.GotSigterm)
        {
            throw new PgException(PgSqlStates.ObjectInUse,
                $"replication slot \"{sub.SlotName}\" is still active for PID {holder} on the provider", null!,
                "Stop the process that holds the slot; the sync catches up again when it restarts.", null!);
        }
    }

    /// <summary>
    /// Waits for a table sync worker's slot to be free. After a second, a walsender of the worker's catch-up, with the
    /// same user and application name, is stopped, as one whose connection has closed holds the slot while it decodes
    /// a transaction. Anything else that holds the slot, such as a session reading its changes, is waited for, with a
    /// warning.
    /// </summary>
    /// <param name="conn">A connection to the provider, outside a local transaction.</param>
    /// <param name="sub">The subscription, with the table's slot name.</param>
    /// <param name="waiter">What waits, as the warning names it.</param>
    /// <param name="afterWait">What happens after the wait, as the warning's hint says.</param>
    /// <param name="wait">How many seconds to wait.</param>
    /// <returns>The process that still holds the slot after the wait, or when the worker is asked to stop, or zero once
    /// the slot is free.</returns>
    private static int WaitForSlot(WorfConnection conn, WorfSubscription sub, string waiter, string afterWait, int wait)
    {
        string catchup = Worf.ApplicationName(sub.Name, CatchupConnection);
        long start = Worker.GetCurrentTimestamp();
        long checkAt = start + 1000000L;
        int holder = 0;
        int warned = 0;
        while (!Worker.GotSigterm)
        {
            holder = PgBackgroundWorker.RunTransaction(() => Rpc.RemoteSlotHolder(conn, sub.SlotName));
            long now = Worker.GetCurrentTimestamp();
            if (holder == 0 || now - start >= wait * 1000000L)
            {
                break;
            }

            if (now >= checkAt)
            {
                checkAt = now + 1000000L;
                int current = holder;
                string? notStopped = PgBackgroundWorker.RunTransaction(() => Rpc.TerminateSlotHolder(conn, sub.SlotName, current, catchup));
                if (notStopped is not null && warned != holder)
                {
                    warned = holder;
                    WorfLog.Warning(new PgDiagnostic($"replication slot \"{sub.SlotName}\" is active for PID {holder} on the provider, which {waiter} waits for")
                    {
                        Detail = notStopped,
                        Hint = afterWait,
                    });
                }
            }

            _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(50));
        }

        return holder;
    }

    /// <summary>
    /// Finishes a sync worker's catch-up: ends the stream and drops the table's slot over the connection it streamed
    /// on, as PostgreSQL's table sync does, so that the walsender that decoded the slot, and holds a reference to its
    /// statistics, is the one that drops it, rather than leave the reference behind in a walsender still exiting as
    /// another connection drops the slot and a later slot takes its place; then closes the connection and finishes.
    /// A slot the stream's connection can't drop is dropped as a sync that didn't stream drops it.
    /// </summary>
    /// <param name="streamConn">The connection the worker streamed on.</param>
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    internal static void FinishCatchup(WorfConnection streamConn, string nspname, string relname)
    {
        WorfSubscription sub = Apply.MySubscription!;
        bool dropped = false;
        try
        {
            streamConn.EndStreaming();
            PgBackgroundWorker.RunTransaction(() => streamConn.Execute($"DROP_REPLICATION_SLOT \"{sub.SlotName}\" WAIT"));
            dropped = true;
        }
        catch (PgException error)
        {
            WorfLog.Debug1($"could not drop replication slot \"{sub.SlotName}\" over the stream's connection: {error.Message}");
        }

        streamConn.Dispose();
        SyncWorkerFinish(nspname, relname, dropped);
    }

    /// <summary>
    /// Finishes the sync worker's work.
    /// </summary>
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    /// <param name="slotDropped">Whether the worker dropped the table's slot already.</param>
    internal static void SyncWorkerFinish(string nspname, string relname, bool slotDropped = false)
    {
        WorfSubscription sub = Apply.MySubscription!;

        // Any outstanding transaction committed when its callback returned, and published its statistics.

        // And flush all writes.
        unsafe
        {
            NativeMethods.XLogFlush(NativeMethods.GetXLogWriteRecPtr());
        }

        SyncWorkerCleanup(sub, slotDropped);

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

        WorfLog.Log($"finished sync of table {nspname}.{relname} for subscriber {sub.Name}");
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

            // Check if the worker is still alive - no point waiting if it died. One that failed keeps its slot, for
            // the restart delay, but has no process.
            bool running;
            byte[] serverNspname = Worf.ToServer(nspname);
            byte[] serverRelname = Worf.ToServer(relname);
            using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
            {
                running = guard.Read((in _, workers) =>
                    Worker.WorkerRunning(workers, Worker.SyncFind(workers, myDatabaseId, subid, serverNspname, serverRelname)));
            }

            if (!running)
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
    /// Waits while a table's sync worker catches up, which it does up to the position an apply worker gave it. A sync
    /// worker that copies the table again, as one that can't catch up from its first copy does, waits for apply first,
    /// so it isn't waited for.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    internal static void WaitForCatchup(uint subid, string nspname, string relname)
    {
        uint myDatabaseId;
        unsafe
        {
            myDatabaseId = NativeGlobals.MyDatabaseId;
        }

        byte[] serverNspname = Worf.ToServer(nspname);
        byte[] serverRelname = Worf.ToServer(relname);
        while (!Worker.GotSigterm &&
            PgBackgroundWorker.RunTransaction(() => GetTableSyncStatus(subid, nspname, relname, true)?.Status == SyncStatusCatchup))
        {
            using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
            {
                int sync = guard.Read((in _, workers) => Worker.SyncFind(workers, myDatabaseId, subid, serverNspname, serverRelname));
                if (sync < 0 || guard[sync].Worker.Sync.CopyLsn != 0)
                {
                    return;
                }
            }

            _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(1000));
            PgInterrupts.Check();
        }
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
    private static string EnsureReplicationSlotSnapshot(WorfSubscription sub, WorfConnection replConn, string appName,
        bool useFailoverSlot, out ulong lsn)
    {
        using WorfConnection sqlConn = Worf.Connect(sub.OriginIf.Dsn, sub.Name, appName);
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
    private static string EnsureReplicationSlotSnapshot(WorfConnection sqlConn, WorfConnection replConn,
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
    private static ushort EnsureReplicationOrigin(WorfSubscription sub) => WorfNode.EnsureOrigin(sub.SlotName, sub.Origin.Id, sub.Id);

    /// <summary>
    /// Transaction management for COPY.
    /// </summary>
    private static void StartCopyOriginTx(WorfConnection conn, string? snapshot)
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
            setupQuery.Add($"SET TRANSACTION SNAPSHOT {Worf.QuoteLiteral(snapshot)}");
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

        Worf.Execute("SET LOCAL session_replication_role = 'replica'");
        SetCopyTargetStyle(false);
        Worf.Execute("SET LOCAL statement_timeout = 0");
        Worf.Execute("SET LOCAL lock_timeout = 0");
    }

    /// <summary>
    /// Sets the styles values take in the copy's target transaction: the origin's, or this node's own, which values
    /// that convert from the provider's type follow, as in replication.
    /// </summary>
    private static void SetCopyTargetStyle(bool own)
    {
        Worf.Execute($"SET LOCAL DATESTYLE TO {(own ? "DEFAULT" : "ISO")}");
        Worf.Execute($"SET LOCAL INTERVALSTYLE TO {(own ? "DEFAULT" : "POSTGRES")}");
        Worf.Execute($"SET LOCAL extra_float_digits TO {(own ? "DEFAULT" : "3")}");
    }

    private static void FinishCopyOriginTx(WorfConnection conn)
    {
        // Close the transaction and connection on origin node.
        try
        {
            conn.Execute("ROLLBACK");
        }
        catch (PgRemoteException error)
        {
            WorfLog.Warning($"ROLLBACK on origin node failed: ERROR:  {error.Message}\n");
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

        Worf.ReploriginSessionOriginLsn = 0;
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
    /// Finds the copied columns whose values convert from the provider's type, other than those whose text the
    /// column type's input reads as converting it would.
    /// </summary>
    /// <returns>Each local column's conversion, by its number less one, or null when no column converts.</returns>
    private static unsafe WorfColumnConversion?[]? FindCopyConversions(WorfRelation rel)
    {
        TupleDescData* desc = rel.Rel->rd_att;
        WorfColumnConversion?[] conversions = new WorfColumnConversion?[desc->natts];
        bool converts = false;

        for (int attnum = 0; attnum < desc->natts; attnum++)
        {
            int remoteattnum = PhysattInAttmap(rel, attnum);
            if (!NativeMethods.TupleDescAttr(desc, attnum)->attisdropped && remoteattnum >= 0 &&
                rel.Conversions[remoteattnum] is { } conversion && conversion.Converts('t') && !conversion.ReadsTextDirectly)
            {
                conversions[attnum] = conversion;
                converts = true;
            }
        }

        return converts ? conversions : null;
    }

    /// <summary>
    /// Builds the COPY TO that reads a table's rows on the provider, for the columns the table has here, with the
    /// provider's column types.
    /// </summary>
    /// <returns>The local table, the columns that receive the copy's, or none for all of them, each local column's
    /// conversion, by its number less one, or null when no column converts, and the query.</returns>
    private static (WorfRelation Rel, List<string> Columns, WorfColumnConversion?[]? Conversions, string Query) CopyQuery(
        WorfConnection originConn, WorfRemoteRel remoterel, List<string> replicationSets)
    {
        // Build the relation map, with the provider's column types and, for money, how it formats it.
        WorfRemoteType?[]? types = Rpc.GetRemoteColumnTypes(originConn, remoterel);
        if (types is not null && types.Any(type => type is WorfRemoteType remote && RelCache.SendsMoney(remote)))
        {
            RelCache.ProviderMoneyFormat = Rpc.GetRemoteMoneyFormat(originConn);
        }

        RelCache.RelationCacheUpdater(remoterel, types);
        WorfRelation rel = RelCache.RelationOpen(remoterel.Relid, PgLockMode.AccessShare);
        List<string> attnamelist = MakeCopyAttnamelist(rel);
        WorfColumnConversion?[]? converted = attnamelist.Count != 0 ? FindCopyConversions(rel) : null;

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

            attlist.Append(Worf.QuoteIdentifier(attname));
        }

        RelCache.RelationClose(rel, PgLockMode.AccessShare);

        // Build COPY TO query.
        StringBuilder query = new("COPY ");

        // If the table is row-filtered we need to run query over the table to execute the filter.
        if (remoterel.HasRowFilter)
        {
            string relname = $"{Worf.QuoteIdentifier(remoterel.Nspname)}.{Worf.QuoteIdentifier(remoterel.Relname)}";

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

                repsetarr.Append(Worf.QuoteLiteral(repsetName));
            }

            query.Append(CultureInfo.InvariantCulture, $"(SELECT {(attnamelist.Count != 0 ? attlist.ToString() : "*")} FROM worf.table_data_filtered(NULL::{relname}, {Worf.QuoteLiteral(relname)}::regclass, ARRAY[{repsetarr}])) ");
        }
        else
        {
            // Otherwise just copy the table.
            query.Append(CultureInfo.InvariantCulture, $"{Worf.QuoteIdentifier(remoterel.Nspname)}.{Worf.QuoteIdentifier(remoterel.Relname)} ");

            if (attnamelist.Count != 0)
            {
                query.Append(CultureInfo.InvariantCulture, $"({attlist}) ");
            }
        }

        query.Append("TO stdout");

        return (rel, attnamelist, converted, query.ToString());
    }

    /// <summary>
    /// Starts a COPY TO on the provider.
    /// </summary>
    private static void StartCopy(WorfConnection originConn, string query)
    {
        try
        {
            originConn.StartCopy(query);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, "table copy failed",
                detail: $"Query '{query}': ERROR:  {error.Message}\n");
        }
    }

    /// <summary>
    /// Starts a COPY TO on the provider and copies its rows into a table: the local one, or the one a merge reads.
    /// </summary>
    /// <param name="originConn">The provider connection, in the copy's transaction.</param>
    /// <param name="rel">The local table.</param>
    /// <param name="into">The table the rows go into.</param>
    /// <param name="attnamelist">The columns that receive the copy's, or none for all of them.</param>
    /// <param name="converted">Each local column's conversion, by its number less one, or null when no column
    /// converts.</param>
    /// <param name="query">The COPY TO.</param>
    /// <param name="lsn">The position the copy is consistent with.</param>
    private static void CopyRows(WorfConnection originConn, WorfRelation rel, uint into, List<string> attnamelist,
        WorfColumnConversion?[]? converted, string query, ulong lsn)
    {
        StartCopy(originConn, query);

        // Execute COPY FROM. A table whose values convert is copied with their conversions, under this node's own
        // settings, which conversions follow, as in replication, and so is any table under worf.exception_behaviour
        // other than error, which skips or records the rows that fail.
        if (converted is null && (Apply.ExceptionBehaviour == WorfExceptionBehaviour.Error || attnamelist.Count == 0))
        {
            _ = originConn.CopyInto(into, attnamelist.Count != 0 ? attnamelist : null);
        }
        else
        {
            SetCopyTargetStyle(converted is not null);
            _ = CopyConversion.Copy(originConn, rel, into, attnamelist, converted, lsn, () => StartCopy(originConn, query));
            SetCopyTargetStyle(false);
        }
    }

    /// <summary>
    /// COPY single table over wire. Runs in the target transaction.
    /// </summary>
    private static void CopyTableData(WorfConnection originConn, WorfRemoteRel remoterel, List<string> replicationSets,
        ulong lsn)
    {
        (WorfRelation rel, List<string> attnamelist, WorfColumnConversion?[]? converted, string query) =
            CopyQuery(originConn, remoterel, replicationSets);
        CopyRows(originConn, rel, rel.Reloid, attnamelist, converted, query, lsn);
        CheckCopiedDeferrable(rel, lsn);

        WorfLog.Info($"finished synchronization of data for table {remoterel.Nspname}.{remoterel.Relname}");
    }

    /// <summary>
    /// Copies a table that has rows again, in the target transaction, and merges the copy into them, as a table sync
    /// does that can't catch up from its first copy. The provider's rows replace the table's with the same key, or join
    /// them, and the table's other rows stay, since whether this subscription wrote a row can't be told in general, as
    /// without commit timestamps or once it's frozen. A row the copy leaves as it was isn't written again. A unique
    /// index other than the key, deferrable or not, or an exclusion constraint, that a row here holds a value in that
    /// one of the provider's takes fails the merge, with a hint to resynchronize, and is recorded, or disables the
    /// subscription, as worf.exception_behaviour says. Writers go on meanwhile, readers see the rows as they were until the merge commits, and
    /// session_replication_role replica keeps foreign keys from being checked, as for apply.
    /// </summary>
    /// <param name="originConn">The provider connection, in the copy's transaction.</param>
    /// <param name="remoterel">The table.</param>
    /// <param name="replicationSets">The replication sets, whose row filters apply.</param>
    /// <param name="lsn">The position the copy is consistent with.</param>
    private static void MergeTableData(WorfConnection originConn, WorfRemoteRel remoterel, List<string> replicationSets,
        ulong lsn)
    {
        (WorfRelation rel, List<string> attnamelist, WorfColumnConversion?[]? converted, string query) =
            CopyQuery(originConn, remoterel, replicationSets);
        uint reloid = rel.Reloid;
        string table = Worf.QuoteQualifiedIdentifier(remoterel.Nspname, remoterel.Relname);
        (bool partitioned, List<string> key, uint index) = TableKey(reloid);
        string only = partitioned ? "" : "ONLY ";

        // The provider's rows go into a table of the copy's columns first.
        StringBuilder create = new("CREATE TEMPORARY TABLE worf_sync_copy ON COMMIT DROP AS SELECT ");
        for (int i = 0; i < attnamelist.Count; i++)
        {
            create.Append(i == 0 ? "" : ", ").Append(Worf.QuoteIdentifier(attnamelist[i]));
        }

        create.Append(attnamelist.Count == 0 ? "*" : "").Append(" FROM ONLY ").Append(table).Append(" WITH NO DATA");
        _ = Worf.Execute(create.ToString());
        uint copyOid = Worf.ReadScalar<uint>($"SELECT '{MergeCopy}'::regclass::oid");

        // The columns the merge writes: the copy's, but those the table generates.
        WorfRows attributes = Worf.Read("""
            SELECT c.attname::text FROM pg_catalog.pg_attribute c
                JOIN pg_catalog.pg_attribute t ON t.attrelid = $2 AND t.attname = c.attname AND t.attgenerated = ''
            WHERE c.attrelid = $1 AND c.attnum > 0 AND NOT c.attisdropped ORDER BY c.attnum
            """, 0, SpiParameter.Create(copyOid), SpiParameter.Create(reloid));
        List<string> columns = [.. Enumerable.Range(0, attributes.Count).Select(i => attributes[i].Get<string>(0))];
        if (key.Count == 0 || !key.TrueForAll(columns.Contains))
        {
            throw Unmergeable(remoterel.Nspname, remoterel.Relname);
        }

        CopyRows(originConn, rel, copyOid, attnamelist, converted, query, lsn);
        _ = Worf.Execute($"ANALYZE {MergeCopy}");

        // The copy's rows replace the table's with the same key, or join them. A MERGE needs no arbiter index, which a
        // deferrable key can't be, and an identity column takes the copy's value. It runs under the session's search
        // path, as apply's changes do, for the triggers it fires, so it names its operators and types with their
        // schemas: the key's equality operators from its index, and the system's for text.
        Dictionary<short, string> operators = Worf.KeyEqualityOperators(index);
        string[] quoted = [.. columns.Select(Worf.QuoteIdentifier)];
        string[] rest = [.. columns.Where(column => !key.Contains(column)).Select(Worf.QuoteIdentifier)];
        StringBuilder merge = new("MERGE INTO ");
        merge.Append(only).Append(table).Append(" t USING ").Append(MergeCopy).Append(" c ON ")
            .Append(string.Join(" AND ", key.Select(column => $"t.{Worf.QuoteIdentifier(column)} {KeyOperator(operators, reloid, column)} c.{Worf.QuoteIdentifier(column)}")));
        if (rest.Length > 0)
        {
            merge.Append(" WHEN MATCHED AND ROW(").Append(string.Join(", ", rest.Select(column => $"t.{column}"))).Append(")::pg_catalog.text OPERATOR(pg_catalog.<>) ROW(")
                .Append(string.Join(", ", rest.Select(column => $"c.{column}"))).Append(")::pg_catalog.text THEN UPDATE SET ")
                .Append(string.Join(", ", rest.Select(column => $"{column} = c.{column}")));
        }

        merge.Append(" WHEN NOT MATCHED THEN INSERT (").Append(string.Join(", ", quoted)).Append(") OVERRIDING SYSTEM VALUE VALUES (")
            .Append(string.Join(", ", quoted.Select(column => $"c.{column}"))).Append(')');

        // A row here can hold a value that another unique index or an exclusion constraint needs for one of the
        // provider's: a row the provider no longer has, or one whose value the provider moved to another row, as a
        // swap of two rows' values does, which the merge writes one row at a time. Under session_replication_role
        // replica, the recheck of a deferrable unique index or exclusion constraint doesn't fire, so the merge checks
        // those itself. Under discard and transdiscard, a merge that fails is merged again in halves, down to the rows
        // that fail alone, which are skipped and recorded as the copy commits, so that the provider's other rows, and
        // the changes after the copy, still arrive.
        string source = $"(SELECT * FROM {MergeCopy} WHERE ctid OPERATOR(pg_catalog.=) ANY ($1::pg_catalog.tid[]))";
        string mergePart = merge.ToString().Replace($" USING {MergeCopy} c ON ", $" USING {source} c ON ", StringComparison.Ordinal);
        try
        {
            PgTransaction.RunInSubtransaction(() =>
            {
                ExecuteQuoted(merge.ToString());
                CheckDeferrableUnique(reloid, only + table);
                CheckDeferrableExclusion(reloid, only + table);
            }, PgSubtransactionMode.Atomic);
        }
        catch (PgException error) when (error.SqlState is PgSqlStates.UniqueViolation or PgSqlStates.ExclusionViolation &&
            Apply.ExceptionBehaviour is not (WorfExceptionBehaviour.Error or WorfExceptionBehaviour.SubDisable))
        {
            WorfRows rows = Worf.Read($"SELECT ctid::text FROM {MergeCopy} ORDER BY ctid", 0);
            List<(string Place, PgException Error)> skipped = [];
            MergeParts(reloid, only + table, mergePart, [.. rows.Select(row => row.Get<string>(0))], skipped);
            RecordSkipped(rel, lsn, skipped);
            WorfLog.Warning(new PgDiagnostic($"skipped {skipped.Count} of the rows of the copy of table {remoterel.Nspname}.{remoterel.Relname} as it merged into the rows it has: {error.Message}")
            {
                Detail = error.Detail,
                Hint = "Resynchronize the table with worf.alter_subscription_resynchronize_table to give it the provider's rows.",
            });
        }
        catch (PgException error) when (error.SqlState is PgSqlStates.UniqueViolation or PgSqlStates.ExclusionViolation)
        {
            PgException failed = new(error.SqlState,
                $"could not merge the copy of table {remoterel.Nspname}.{remoterel.Relname} into the rows it has: {error.Message}",
                detail: error.Detail,
                hint: "Rows here hold values the provider's rows take, as rows the provider no longer has, or rows whose values it moved to others. Resynchronize the table with worf.alter_subscription_resynchronize_table, which empties it and copies it again.");
            CopyConversion.DisableAfterMerge(rel, lsn, failed);
            throw failed;
        }

        _ = Worf.Execute($"DROP TABLE {MergeCopy}");

        WorfLog.Info($"finished synchronization of data for table {remoterel.Nspname}.{remoterel.Relname}");
    }

    /// <summary>
    /// Merges some of a copy's rows, by their places in it, in a subtransaction, and in halves when that fails, down to
    /// a row that fails alone, whose place is kept to record.
    /// </summary>
    private static void MergeParts(uint reloid, string table, string merge, List<string> rows, List<(string Place, PgException Error)> skipped)
    {
        try
        {
            PgTransaction.RunInSubtransaction(() =>
            {
                _ = Worf.ExecuteUnpinned(merge, SpiParameter.Create(rows.ToArray()));
                CheckDeferrableUnique(reloid, table);
                CheckDeferrableExclusion(reloid, table);
            }, PgSubtransactionMode.Atomic);
        }
        catch (PgException error) when (error.SqlState is PgSqlStates.UniqueViolation or PgSqlStates.ExclusionViolation)
        {
            if (rows.Count > 1)
            {
                MergeParts(reloid, table, merge, rows[..(rows.Count / 2)], skipped);
                MergeParts(reloid, table, merge, rows[(rows.Count / 2)..], skipped);
                return;
            }

            skipped.Add((rows[0], error));
        }
    }

    /// <summary>
    /// Records the rows of a table's copy that its merge skipped, by their places in the copy, with their errors, from
    /// the copy, a thousand at a time, and, of a thousand the log can't hold, each alone.
    /// </summary>
    private static void RecordSkipped(WorfRelation rel, ulong lsn, List<(string Place, PgException Error)> skipped)
    {
        const string Rows = $"""
            SELECT pg_catalog.row_to_json(c)::pg_catalog.text, f.error_sqlstate, f.error_message
            FROM ROWS FROM (pg_catalog.unnest($1::pg_catalog.tid[]), pg_catalog.unnest($2::pg_catalog.text[]), pg_catalog.unnest($3::pg_catalog.text[]))
                WITH ORDINALITY AS f (place, error_sqlstate, error_message, n)
                JOIN {MergeCopy} c ON c.ctid OPERATOR(pg_catalog.=) f.place
            ORDER BY f.n
            """;
        WorfFailedChange change = CopiedChange(rel, lsn, "MERGE");
        foreach ((string Place, PgException Error)[] run in skipped.Chunk(1000))
        {
            if (Record(run))
            {
                continue;
            }

            int unrecorded = run.Count(row => !Record([row]));
            if (unrecorded > 0)
            {
                WorfLog.Warning($"could not record {unrecorded} of the rows the merge of table {rel.Nspname}.{rel.Relname}'s copy skipped in worf.exception_log");
            }
        }

        bool Record((string Place, PgException Error)[] rows) => ExceptionLog.TryRecordCopied(change, Rows,
            SpiParameter.Create(rows.Select(row => row.Place).ToArray()),
            SpiParameter.Create(rows.Select(row => row.Error.SqlState).ToArray()),
            SpiParameter.Create(rows.Select(row => row.Error.Message).ToArray()));
    }

    /// <summary>
    /// Describes a table's copy as the exception log records its rows that fail.
    /// </summary>
    private static WorfFailedChange CopiedChange(WorfRelation rel, ulong lsn, string operation)
    {
        WorfSubscription sub = Apply.MySubscription!;
        long started;
        unsafe
        {
            started = NativeMethods.GetCurrentTransactionStartTimestamp();
        }

        return new WorfFailedChange
        {
            Subscription = sub,
            WriterNodeId = sub.Origin.Id,
            CommitTime = started,
            CommitLsn = lsn,
            Operation = operation,
            Relation = rel,
        };
    }

    /// <summary>
    /// Checks a table's copy against its deferrable unique and exclusion constraints, whose rechecks COPY under
    /// session_replication_role replica doesn't fire: a conflict fails the copy, or, as worf.exception_behaviour skips
    /// or records rows, the later rows of each conflict are deleted and recorded, and the subscription is disabled
    /// under sub_disable.
    /// </summary>
    private static void CheckCopiedDeferrable(WorfRelation rel, ulong lsn)
    {
        bool partitioned;
        unsafe
        {
            partitioned = NativeMethods.get_rel_relkind(rel.Reloid) == (sbyte)'p';
        }

        string table = (partitioned ? "" : "ONLY ") + Worf.QuoteQualifiedIdentifier(rel.Nspname, rel.Relname);
        try
        {
            PgTransaction.RunInSubtransaction(() =>
            {
                CheckDeferrableUnique(rel.Reloid, table);
                CheckDeferrableExclusion(rel.Reloid, table);
            }, PgSubtransactionMode.Atomic);
        }
        catch (PgException error) when (error.SqlState is PgSqlStates.UniqueViolation or PgSqlStates.ExclusionViolation &&
            Apply.ExceptionBehaviour is not (WorfExceptionBehaviour.Error or WorfExceptionBehaviour.SubDisable))
        {
            DeleteDeferrableConflicts(rel.Reloid, table, CopiedChange(rel, lsn, "COPY"));
        }
        catch (PgException error) when (error.SqlState is PgSqlStates.UniqueViolation or PgSqlStates.ExclusionViolation &&
            Apply.ExceptionBehaviour == WorfExceptionBehaviour.SubDisable)
        {
            CopyConversion.DisableAfterMerge(rel, lsn, error, "COPY");
            throw;
        }
    }

    /// <summary>
    /// Deletes the rows of a table that conflict with an earlier row, by its place in the table, in a deferrable unique
    /// or exclusion constraint, and records them, as they're deleted, with the conflict each fails, or, when the log
    /// can't hold one of them, each alone.
    /// </summary>
    private static void DeleteDeferrableConflicts(uint reloid, string table, WorfFailedChange change)
    {
        WorfRows constraints = Worf.Read("""
            SELECT c.conname::text, c.contype = 'x',
                (SELECT pg_catalog.array_agg(pg_catalog.pg_get_indexdef(i.indexrelid, k, false) ORDER BY k) FROM pg_catalog.generate_series(1, i.indnkeyatts) k),
                (SELECT pg_catalog.array_agg(pg_catalog.format('OPERATOR(%I.%s)', n.nspname, o.oprname) ORDER BY e.k)
                    FROM pg_catalog.unnest(c.conexclop) WITH ORDINALITY e (op, k)
                        JOIN pg_catalog.pg_operator o ON o.oid = e.op JOIN pg_catalog.pg_namespace n ON n.oid = o.oprnamespace),
                pg_catalog.pg_get_expr(i.indpred, i.indrelid), i.indnullsnotdistinct
            FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_index i ON i.indexrelid = c.conindid
            WHERE c.conrelid = $1 AND c.contype IN ('p', 'u', 'x') AND c.condeferrable
            """, 0, SpiParameter.Create(reloid));
        foreach (WorfRow constraint in constraints)
        {
            string name = constraint.Get<string>(0);
            bool exclusion = constraint.Get<bool>(1);
            string?[] keys = constraint.Get<string?[]>(2);
            List<string> conditions = [.. constraint.Get<string?>(4) is string predicate ? [$"({predicate})"] : Array.Empty<string>()];
            if (!exclusion && !constraint.Get<bool>(5))
            {
                conditions.AddRange(keys.Select(key => $"({key}) IS NOT NULL"));
            }

            string where = conditions.Count == 0 ? "" : $" WHERE {string.Join(" AND ", conditions)}";
            string rows = $"(SELECT tableoid, ctid, {string.Join(", ", keys.Select((key, k) => $"({key}) AS k{k}"))} FROM {table}{where})";
            string conflicts = exclusion
                ? string.Join(" AND ", constraint.Get<string?[]>(3).Select((op, k) => $"a.k{k} {op} b.k{k}"))
                : string.Join(" AND ", keys.Select((_, k) => $"a.k{k} IS NOT DISTINCT FROM b.k{k}"));
            string conflicting = $"SELECT b.tableoid, b.ctid FROM {rows} a JOIN {rows} b ON (a.tableoid, a.ctid) < (b.tableoid, b.ctid) AND {conflicts}";
            PgException error = exclusion
                ? new PgException(PgSqlStates.ExclusionViolation, $"conflicting key value violates exclusion constraint \"{name}\"")
                : new PgException(PgSqlStates.UniqueViolation, $"duplicate key value violates unique constraint \"{name}\"");
            if (ExceptionLog.TryRecordCopied(change, $"DELETE FROM {table} t WHERE (t.tableoid, t.ctid) IN ({conflicting}) {Returning(1)}",
                SpiParameter.Create(error.SqlState), SpiParameter.Create(error.Message)))
            {
                continue;
            }

            foreach (WorfRow place in Worf.Read($"SELECT DISTINCT tableoid, ctid::pg_catalog.text FROM ({conflicting}) c", 0))
            {
                const string One = "t.tableoid OPERATOR(pg_catalog.=) $1 AND t.ctid OPERATOR(pg_catalog.=) $2::pg_catalog.tid";
                SpiParameter tableoid = SpiParameter.Create(place.Get<uint>(0));
                SpiParameter ctid = SpiParameter.Create(place.Get<string>(1));
                if (!ExceptionLog.TryRecordCopied(change, $"DELETE FROM {table} t WHERE {One} {Returning(3)}", tableoid, ctid,
                    SpiParameter.Create(error.SqlState), SpiParameter.Create(error.Message)))
                {
                    _ = Worf.Execute($"DELETE FROM {table} t WHERE {One}", tableoid, ctid);
                    WorfLog.Warning("could not record the failed remote change in worf.exception_log");
                }
            }
        }

        static string Returning(int error) => $"RETURNING pg_catalog.row_to_json(t)::pg_catalog.text, ${error}::pg_catalog.text, ${error + 1}::pg_catalog.text";
    }

    /// <summary>
    /// Fails with a unique violation when two of a table's rows hold the same values in a deferrable unique index, as
    /// the index's recheck would at the end of the statement had session_replication_role let it fire: the same values
    /// of its columns and expressions, among the rows its predicate takes, none of them null unless the index treats
    /// nulls as equal.
    /// </summary>
    /// <param name="reloid">The table.</param>
    /// <param name="table">The table's qualified name, after ONLY when it isn't partitioned.</param>
    private static void CheckDeferrableUnique(uint reloid, string table)
    {
        WorfRows indexes = Worf.Read("""
            SELECT c.conname::text, i.indnkeyatts,
                (SELECT pg_catalog.array_agg(pg_catalog.pg_get_indexdef(i.indexrelid, k, false) ORDER BY k) FROM pg_catalog.generate_series(1, i.indnkeyatts) k),
                pg_catalog.pg_get_expr(i.indpred, i.indrelid), i.indnullsnotdistinct
            FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_index i ON i.indexrelid = c.conindid
            WHERE c.conrelid = $1 AND c.contype IN ('p', 'u') AND c.condeferrable
            """, 0, SpiParameter.Create(reloid));
        foreach (WorfRow index in indexes)
        {
            string[] keys = [.. index.Get<string?[]>(2).Select(key => $"({key})")];
            List<string> conditions = [.. index.Get<string?>(3) is string predicate ? [$"({predicate})"] : Array.Empty<string>()];
            if (!index.Get<bool>(4))
            {
                conditions.AddRange(keys.Select(key => $"{key} IS NOT NULL"));
            }

            string where = conditions.Count == 0 ? "" : $" WHERE {string.Join(" AND ", conditions)}";
            if (Worf.ReadScalar<bool>($"SELECT EXISTS (SELECT FROM {table}{where} GROUP BY {string.Join(", ", keys)} HAVING pg_catalog.count(*) > 1)"))
            {
                throw new PgException(PgSqlStates.UniqueViolation, $"duplicate key value violates unique constraint \"{index.Get<string>(0)}\"");
            }
        }
    }

    /// <summary>
    /// Fails with an exclusion violation when two of a table's rows conflict in a deferrable exclusion constraint, as
    /// its recheck would at the end of the statement had session_replication_role let it fire: every key of one,
    /// among the rows the index's predicate takes, compares true with the other's by the constraint's operator.
    /// </summary>
    /// <param name="reloid">The table.</param>
    /// <param name="table">The table's qualified name, after ONLY when it isn't partitioned.</param>
    private static void CheckDeferrableExclusion(uint reloid, string table)
    {
        WorfRows constraints = Worf.Read("""
            SELECT c.conname::text,
                (SELECT pg_catalog.array_agg(pg_catalog.pg_get_indexdef(i.indexrelid, k, false) ORDER BY k) FROM pg_catalog.generate_series(1, i.indnkeyatts) k),
                (SELECT pg_catalog.array_agg(pg_catalog.format('OPERATOR(%I.%s)', n.nspname, o.oprname) ORDER BY e.k)
                    FROM pg_catalog.unnest(c.conexclop) WITH ORDINALITY e (op, k)
                        JOIN pg_catalog.pg_operator o ON o.oid = e.op JOIN pg_catalog.pg_namespace n ON n.oid = o.oprnamespace),
                pg_catalog.pg_get_expr(i.indpred, i.indrelid)
            FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_index i ON i.indexrelid = c.conindid
            WHERE c.conrelid = $1 AND c.contype = 'x' AND c.condeferrable
            """, 0, SpiParameter.Create(reloid));
        foreach (WorfRow constraint in constraints)
        {
            string?[] keys = constraint.Get<string?[]>(1);
            string?[] operators = constraint.Get<string?[]>(2);
            string where = constraint.Get<string?>(3) is string predicate ? $" WHERE ({predicate})" : "";
            string rows = $"(SELECT tableoid, ctid, {string.Join(", ", keys.Select((key, k) => $"({key}) AS k{k}"))} FROM {table}{where})";
            string conflicts = string.Join(" AND ", operators.Select((op, k) => $"a.k{k} {op} b.k{k}"));
            if (Worf.ReadScalar<bool>($"SELECT EXISTS (SELECT FROM {rows} a JOIN {rows} b ON (a.tableoid, a.ctid) <> (b.tableoid, b.ctid) AND {conflicts})"))
            {
                throw new PgException(PgSqlStates.ExclusionViolation, $"conflicting key value violates exclusion constraint \"{constraint.Get<string>(0)}\"");
            }
        }
    }

    /// <summary>
    /// Fails a sync that would copy a table again into the rows it has, as one that can't catch up from its first copy
    /// does, when the table has no key to match the copy's rows with its own by.
    /// </summary>
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    internal static void RequireMergeable(string nspname, string relname)
    {
        if (Worf.ReadScalar<uint?>("SELECT to_regclass($1)::oid", SpiParameter.Create(Worf.QuoteQualifiedIdentifier(nspname, relname))) is uint reloid &&
            TableKey(reloid).Key.Count == 0)
        {
            throw Unmergeable(nspname, relname);
        }
    }

    private static PgException Unmergeable(string nspname, string relname)
        => new(PgSqlStates.ObjectNotInPrerequisiteState,
            $"could not copy table {nspname}.{relname} again into the rows it has, as it has no replica identity index or primary key that the copy has",
            hint: "Resynchronize the table with worf.alter_subscription_resynchronize_table, which empties it and copies it again.");

    /// <summary>
    /// Runs the MERGE a merge composes, whose every name the merge quotes with <see cref="Worf.QuoteIdentifier"/> and
    /// qualifies, and that binds no values, under the session's search path, for the triggers it fires.
    /// </summary>
    private static void ExecuteQuoted(string sql) => _ = Worf.ExecuteUnpinned(sql);

    /// <summary>
    /// Gets the equality operator a key column's index compares it with, written with its schema.
    /// </summary>
    private static unsafe string KeyOperator(Dictionary<short, string> operators, uint reloid, string column)
        => operators.TryGetValue(NativeMethods.get_attnum(reloid, Worf.PStrDup(column)), out string? op) ? op : "OPERATOR(pg_catalog.=)";

    /// <summary>
    /// Gets whether a table is partitioned, and its key's columns and index: those of its replica identity index or else
    /// its primary key, even a deferrable one, which PostgreSQL doesn't take as the replica identity, or none.
    /// </summary>
    private static unsafe (bool Partitioned, List<string> Key, uint Index) TableKey(uint reloid)
    {
        RelationData* rel = NativeMethods.table_open(reloid, AccessShareLock);
        bool partitioned = rel->rd_rel->relkind == (sbyte)'p';
        uint index = NativeMethods.RelationGetReplicaIndex(rel);
        if (index == 0)
        {
            index = rel->rd_pkindex;
        }

        NativeMethods.table_close(rel, NoLock);
        if (index == 0)
        {
            index = Worf.ReadScalar<uint?>("SELECT indexrelid FROM pg_catalog.pg_index WHERE indrelid = $1 AND indisprimary",
                SpiParameter.Create(reloid)) ?? 0;
        }

        if (index == 0)
        {
            return (partitioned, [], 0);
        }

        WorfRows key = Worf.Read("""
            SELECT a.attname::text FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_attribute a
                    ON a.attrelid = i.indrelid AND a.attnum = ANY ((i.indkey::smallint[])[0:i.indnkeyatts - 1])
            WHERE i.indexrelid = $1
            """, 0, SpiParameter.Create(index));
        return (partitioned, [.. Enumerable.Range(0, key.Count).Select(i => key[i].Get<string>(0))], index);
    }

    /// <summary>
    /// Copy data from origin node to target node. Creates new connection to origin.
    /// </summary>
    /// <param name="subName">The subscription's name.</param>
    /// <param name="originDsn">The provider's connection string.</param>
    /// <param name="originSnapshot">The snapshot the copy reads.</param>
    /// <param name="tables">The tables.</param>
    /// <param name="replicationSets">The replication sets, whose row filters apply.</param>
    /// <param name="originName">The replication origin the copy commits under.</param>
    /// <param name="lsn">The position the copy is consistent with.</param>
    /// <param name="merge">Whether the copy merges into tables that have rows, whose commit advances the origin to
    /// <paramref name="lsn"/>, rather than going into empty tables.</param>
    /// <param name="copied">Runs in the copy's transaction once the tables are copied.</param>
    private static void CopyTablesData(string subName, string originDsn, string originSnapshot,
        List<(string Nspname, string Relname)> tables, List<string> replicationSets, string originName, ulong lsn,
        bool merge, Action copied)
    {
        // Connect to origin node.
        WorfConnection originConn = Worf.Connect(originDsn, subName, "copy");
        StartCopyOriginTx(originConn, originSnapshot);

        // Copy every table in the target transaction.
        try
        {
            PgBackgroundWorker.RunTransaction(() =>
            {
                StartCopyTargetTx(originName);
                if (merge)
                {
                    Worf.ReploriginSessionOriginLsn = lsn;
                }

                foreach ((string nspname, string relname) in tables)
                {
                    WorfRemoteRel remoterel = Rpc.GetRemoteRepsetTable(originConn, nspname, relname, replicationSets);

                    if (merge)
                    {
                        MergeTableData(originConn, remoterel, replicationSets, lsn);
                    }
                    else
                    {
                        CopyTableData(originConn, remoterel, replicationSets, lsn);
                    }

                    PgInterrupts.Check();
                }

                copied();
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
    /// Reads the provider's current WAL position.
    /// </summary>
    private static ulong ProviderPosition(WorfSubscription sub)
    {
        using WorfConnection conn = Worf.Connect(sub.OriginIf.Dsn, sub.Name, "copy_position");
        string? position = PgBackgroundWorker.RunTransaction(() =>
        {
            try
            {
                return conn.Query("SELECT pg_catalog.pg_current_wal_lsn()", 1)[0][0];
            }
            catch (PgRemoteException error)
            {
                throw new PgException(PgSqlStates.InternalError, $"could not read the provider's WAL position: ERROR:  {error.Message}\n");
            }
        });

        return Worf.TryParseLsn(position ?? "", out ulong lsn) ? lsn
            : throw new PgException(PgSqlStates.InvalidTextRepresentation, $"invalid input syntax for type pg_lsn: \"{position}\"");
    }

    /// <summary>
    /// Waits for the subscription's apply worker to apply everything that committed before a position. Queued SQL that
    /// committed before the copy's snapshot has to change the table before the copy does, as it changed the provider's
    /// rows before the snapshot; run after the copy, it changes the copied rows again.
    /// </summary>
    /// <param name="sub">The subscription.</param>
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    /// <param name="lsn">The position.</param>
    /// <param name="stallTimeout">How long apply may get no further, as when it fails on a change, before the wait
    /// stops.</param>
    /// <param name="waitingSince">When the copy started to wait for apply. Once it has waited a while, each wait says
    /// so as it starts, and then now and again.</param>
    /// <returns>Whether apply reached the position.</returns>
    private static bool WaitForApply(WorfSubscription sub, string nspname, string relname, ulong lsn, TimeSpan stallTimeout,
        long waitingSince)
    {
        PublishCopyLsn(lsn);
        long progressed = Stopwatch.GetTimestamp();
        TimeSpan logAfter = ApplyWaitLogAfter;
        ulong applied = 0;
        ulong change = 0;
        while (true)
        {
            WorfSyncWorker reported;
            using (PgLwLockShareGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Share())
            {
                reported = guard[Worker.MyWorfWorker].Worker.Sync;
            }

            if (reported.AppliedLsn >= lsn)
            {
                PublishCopyLsn(0);
                return true;
            }

            // Apply gets further with each transaction, and with each change of one that takes a while.
            if (reported.AppliedLsn != applied || reported.ChangeLsn != change)
            {
                applied = reported.AppliedLsn;
                change = reported.ChangeLsn;
                progressed = Stopwatch.GetTimestamp();
            }
            else if (Stopwatch.GetElapsedTime(progressed) >= stallTimeout)
            {
                PublishCopyLsn(0);
                return false;
            }

            if (Stopwatch.GetElapsedTime(waitingSince) >= logAfter)
            {
                WorfLog.Log($"table {nspname}.{relname} waits to copy until subscription {sub.Name} applies up to {Worf.FormatLsn(lsn)}, " +
                    (applied == 0 ? "and it hasn't said how far it is" : $"and it's at {Worf.FormatLsn(applied)}"));
                logAfter = Stopwatch.GetElapsedTime(waitingSince) + ApplyWaitLogInterval;
            }

            if (Worker.GotSigterm)
            {
                throw new OperationCanceledException("The worf worker is stopping.");
            }

            _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(1000));
            PgInterrupts.Check();
        }
    }

    /// <summary>
    /// Tells the subscription's apply worker the position the worker waits for it to reach, or zero once it doesn't.
    /// The worker keeps how far it heard apply has got, which apply only adds to, and starts from the position the apply
    /// worker last reported to the provider if that's further, as when apply hasn't got further since.
    /// </summary>
    /// <param name="lsn">The position.</param>
    private static void PublishCopyLsn(ulong lsn)
    {
        using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive();
        WorfWorker me = guard[Worker.MyWorfWorker];
        me.Worker.Sync.CopyLsn = lsn;

        int apply = guard.Read((in _, workers) => Worker.ApplyFind(workers, me.Dboid, me.Worker.Apply.SubId));
        if (lsn != 0 && apply >= 0 && guard[apply].Proc != 0)
        {
            if (guard[apply].Worker.Apply.FlushLsn > me.Worker.Sync.AppliedLsn)
            {
                me.Worker.Sync.AppliedLsn = guard[apply].Worker.Apply.FlushLsn;
                me.Worker.Sync.ChangeLsn = 0;
            }

            Worker.SetProcLatch(guard[apply].Proc);
        }

        guard[Worker.MyWorfWorker] = me;
    }

    /// <summary>
    /// Copy data from origin node to target node. Creates new connection to origin. This is basically same as the
    /// <see cref="CopyTablesData"/>, but it can't be easily merged to single function because we need to get list of
    /// tables here after the transaction is bound to a snapshot.
    /// </summary>
    private static void CopyReplicationSetsData(string subName, string originDsn, string originSnapshot,
        List<string> replicationSets, string originName, ulong lsn, Action<List<WorfRemoteRel>> copied)
    {
        // Connect to origin node.
        WorfConnection originConn = Worf.Connect(originDsn, subName, "copy");
        StartCopyOriginTx(originConn, originSnapshot);

        // Get tables to copy from origin node, and copy every table in the target transaction, which records what
        // it copied.
        try
        {
            PgBackgroundWorker.RunTransaction(() =>
            {
                List<WorfRemoteRel> tables = Rpc.GetRemoteRepsetTables(originConn, replicationSets);

                StartCopyTargetTx(originName);
                foreach (WorfRemoteRel remoterel in tables)
                {
                    CopyTableData(originConn, remoterel, replicationSets, lsn);

                    PgInterrupts.Check();
                }

                copied(tables);
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
    /// Drops a table sync worker's slot and origins, in transactions of its own. A slot that something other than the
    /// worker's catch-up still holds after a few seconds, or that can't be dropped, as when the provider can't be
    /// reached, is left to the subscription's apply worker, which drops it once it can, rather than holding up the
    /// subscription's later table syncs, which wait for this worker to exit.
    /// </summary>
    private static void SyncWorkerCleanup(WorfSubscription sub, bool slotDropped)
    {
        // Drop the slot on the remote side, unless the worker did over the connection it streamed on. The connection is
        // made in a transaction, so that a failure to make it can be caught.
        if (!slotDropped)
        {
            string? left;
            try
            {
                using WorfConnection originConn = PgBackgroundWorker.RunTransaction(() => Worf.Connect(sub.OriginIf.Dsn, sub.Name, "cleanup"));
                int holder = WaitForSlot(originConn, sub, "this sync's cleanup",
                    $"The slot is left to the subscription's apply worker after {CleanupSlotWait} seconds.", CleanupSlotWait);
                left = holder != 0 ? $"PID {holder} holds it." : null;
                if (holder == 0)
                {
                    PgBackgroundWorker.RunTransaction(() => Rpc.DropRemoteSlot(originConn, sub.SlotName));
                }
            }
            catch (PgException error)
            {
                left = error.Message;
            }

            if (left is not null)
            {
                LeaveSlot(sub, left);
            }
        }

        // Drop the origin tracking locally.
        if (PgReplicationOrigin.SessionOrigin != 0)
        {
            PgBackgroundWorker.RunTransaction(() =>
            {
                PgReplicationOrigin.ResetSession();
                WorfNode.DropOrigins(sub.SlotName);
            });
        }
    }

    /// <summary>
    /// Counts one more failure of the subscription's initial synchronization in the apply worker's slot, or none
    /// once it succeeds.
    /// </summary>
    private static void CountSyncFailures(bool failed)
    {
        using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive();
        WorfWorker worker = guard[Worker.MyWorfWorker];
        worker.Worker.Apply.SyncFailures = failed ? worker.Worker.Apply.SyncFailures + 1 : 0;
        guard[Worker.MyWorfWorker] = worker;
    }

    /// <summary>
    /// Leaves a table sync worker's slot on the provider to the subscription's apply worker, which drops it once it can,
    /// with a warning. An apply worker that initializes the subscription leaves its own slot to its next try, which
    /// drops it, or to dropping the subscription.
    /// </summary>
    /// <param name="sub">The subscription, with the slot's name.</param>
    /// <param name="reason">Why the slot couldn't be dropped.</param>
    private static void LeaveSlot(WorfSubscription sub, string reason)
    {
        WorfWorker me = Worker.MyWorker;
        bool tableSync = me.WorkerType == WorfWorkerType.Sync;
        WorfLog.Warning(new PgDiagnostic($"could not drop replication slot \"{sub.SlotName}\" on the provider")
        {
            Detail = reason,
            Hint = tableSync ? "The subscription's apply worker drops it once it can."
                : "The subscription's next try to initialize, or dropping the subscription, drops it.",
        });

        if (!tableSync)
        {
            return;
        }

        using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive();
        int apply = guard.Read((in _, workers) => Worker.ApplyFind(workers, me.Dboid, me.Worker.Apply.SubId));
        if (apply >= 0)
        {
            WorfWorker worker = guard[apply];
            worker.Worker.Apply.SyncSlotsLeft = true;
            guard[apply] = worker;
            if (worker.Proc != 0)
            {
                Worker.SetProcLatch(worker.Proc);
            }
        }
    }

    /// <summary>
    /// Gets the name of the slot a table sync worker creates on the provider: the subscription's followed by a hash of
    /// the table's name. No subscription's own slot name ends as one does, so none is taken for another's.
    /// </summary>
    /// <param name="slotName">The subscription's slot name.</param>
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    /// <returns>The slot's name.</returns>
    internal static string SyncSlotName(string slotName, string nspname, string relname)
        => $"{slotName}_{Worf.HashAny(Worf.QuoteQualifiedIdentifier(nspname, relname)):x8}";

    /// <summary>
    /// Gets the names of the slots the table syncs of a subscription's tables create on the provider.
    /// </summary>
    /// <param name="sub">The subscription.</param>
    /// <returns>The slots' names.</returns>
    internal static List<string> SyncSlotNames(WorfSubscription sub)
        => [.. GetSubscriptionTables(sub.Id).Select(sync => SyncSlotName(sub.SlotName, sync.Nspname, sync.Relname))
            .Concat(Worf.Read("SELECT slot_name::text FROM worf.sync_slot WHERE sub_id = $1", 0, SpiParameter.Create(sub.Id)).Select(row => row.Get<string>(0)))
            .Distinct()];

    /// <summary>
    /// Records the slot a table sync is about to make on the provider, in a transaction of its own, so that it is found
    /// to drop once the sync is done with it, though the table's sync status is gone by then, as once the table leaves
    /// the subscription or is dropped here.
    /// </summary>
    /// <param name="sub">The subscription, with the table's slot name.</param>
    private static void RecordSyncSlot(WorfSubscription sub)
        => PgBackgroundWorker.RunTransaction(() => _ = Worf.Execute(
            "INSERT INTO worf.sync_slot VALUES ($1, $2) ON CONFLICT DO NOTHING", SpiParameter.Create(sub.Id), SpiParameter.Create(sub.SlotName)));

    /// <summary>
    /// Drops the slots on the provider that a subscription's table syncs left, but those still in use, and forgets the
    /// ones dropped or gone.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="sub">The subscription.</param>
    /// <returns>The slots that couldn't be dropped.</returns>
    internal static List<string> DropLeftSyncSlots(WorfConnection conn, WorfSubscription sub)
    {
        List<string> names = SyncSlotNames(sub);
        HashSet<string> kept = KeptSyncSlots(sub);
        List<string> left = DropSyncSlots(conn, names, kept);
        string[] gone = [.. names.Where(name => !kept.Contains(name) && !left.Contains(name))];
        if (gone.Length > 0)
        {
            _ = Worf.Execute("DELETE FROM worf.sync_slot WHERE sub_id = $1 AND slot_name = ANY ($2::text[]::name[])",
                SpiParameter.Create(sub.Id), SpiParameter.Create(gone));
        }

        return left;
    }

    /// <summary>
    /// Checks, only reading, whether a subscription has a table sync's slot recorded that no sync uses any more.
    /// </summary>
    /// <param name="sub">The subscription.</param>
    /// <returns>Whether it has.</returns>
    internal static bool RecordedSyncSlotsLeft(WorfSubscription sub)
    {
        WorfRows recorded = Worf.Read("SELECT slot_name::text FROM worf.sync_slot WHERE sub_id = $1", 0, SpiParameter.Create(sub.Id));
        if (recorded.Count == 0)
        {
            return false;
        }

        HashSet<string> kept = KeptSyncSlots(sub);
        return recorded.Any(row => !kept.Contains(row.Get<string>(0)));
    }

    /// <summary>
    /// Gets the slots on the provider that a subscription's table syncs still stream from or drop themselves: those of
    /// tables that are synchronizing, or whose sync worker runs.
    /// </summary>
    /// <param name="sub">The subscription.</param>
    /// <returns>The slots' names.</returns>
    internal static HashSet<string> KeptSyncSlots(WorfSubscription sub)
    {
        HashSet<string> kept = [.. GetUnsyncedTables(sub.Id)
            .Where(static sync => sync.Status != SyncStatusSyncdone)
            .Select(sync => SyncSlotName(sub.SlotName, sync.Nspname, sync.Relname))];

        uint dboid;
        unsafe
        {
            dboid = NativeGlobals.MyDatabaseId;
        }

        using PgLwLockShareGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Share();
        List<int> syncs = guard.Read((in _, workers) => Worker.SyncFindAll(workers, dboid, sub.Id));
        for (int i = 0; i < syncs.Count; i++)
        {
            WorfWorker w = guard[syncs[i]];
            if (w.Proc != 0)
            {
                _ = kept.Add(SyncSlotName(sub.SlotName, Worker.NameStr(w.Worker.Sync.Nspname), Worker.NameStr(w.Worker.Sync.Relname)));
            }
        }

        return kept;
    }

    /// <summary>
    /// Drops the slots on the provider that a subscription's table syncs left, but those kept. One that something holds
    /// stays.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="names">The names of the slots the subscription's table syncs create, from <see cref="SyncSlotNames"/>.</param>
    /// <param name="kept">The slots that stay, as those of tables still synchronizing.</param>
    /// <returns>The slots that couldn't be dropped.</returns>
    internal static List<string> DropSyncSlots(WorfConnection conn, IReadOnlyCollection<string> names, IReadOnlySet<string> kept)
    {
        List<string> left = [];
        foreach ((string name, bool active) in Rpc.RemoteSyncSlots(conn, names))
        {
            if (kept.Contains(name))
            {
                continue;
            }

            try
            {
                if (active)
                {
                    left.Add(name);
                }
                else
                {
                    Rpc.DropRemoteSlot(conn, name);
                }
            }
            catch (PgException)
            {
                left.Add(name);
            }
        }

        return left;
    }

    /// <summary>
    /// Cleans the subscription's slot up after an error, as the error cleanup callback does, once the failed
    /// transaction has rolled back, as an exception filter, which runs before, couldn't start transactions of its own.
    /// </summary>
    private static void CleanupAfterError(WorfSubscription sub)
    {
        try
        {
            SyncWorkerCleanup(sub, false);
        }
        catch (PgException error)
        {
            WorfLog.Warning($"could not clean up after the failed synchronization: {error.Message}");
        }
    }

    private static void SyncTmpfileCleanup(string tmpfile)
    {
        try
        {
            File.Delete(tmpfile);
        }
        catch (IOException error)
        {
            WorfLog.Warning($"Failed to clean up worf temporary dump file \"{tmpfile}\" on exit/error: {error.Message}");
        }
        catch (UnauthorizedAccessException error)
        {
            WorfLog.Warning($"Failed to clean up worf temporary dump file \"{tmpfile}\" on exit/error: {error.Message}");
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

    // The temporary table a merge copies the provider's rows into first.
    private const string MergeCopy = "pg_temp.worf_sync_copy";

    private const int NoLock = 0;
    private const int AccessShareLock = 1;
    private const int RowExclusiveLock = 3;
    private const ushort BtEqualStrategyNumber = 3;
    private const uint FOideq = 184;
    private const uint FNameeq = 62;

    private static string LastErrorMessage()
        => new Win32Exception(Marshal.GetLastSystemError()).Message;
}
