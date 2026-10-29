using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// worf apply logic.
/// </summary>
internal static unsafe partial class Apply
{
    // Number of tuples inserted after which we switch to multi-insert.
    private const int MinMultiInsertTuples = 5;

    // How worf.exception_behaviour applies a failed transaction again: change by change, as a whole in a subtransaction,
    // or, when its commit fails however it's applied, not at all.
    private const int ExceptionByChange = 1;
    private const int ExceptionByTransaction = 2;
    private const int ExceptionWithoutApplying = 3;

    private static readonly LinkedList<(ulong LocalEnd, ulong RemoteEnd)> s_lsnMapping = new();

    private static bool s_inRemoteTransaction;
    private static ulong s_remoteOriginLsn;
    private static ushort s_remoteOriginId;
    private static uint s_writerNodeId;
    private static uint s_sessionWriterNodeId;
    private static bool s_skipRemoteTransaction;
    private static long s_applyDelay;
    private static uint s_queueRelid;
    private static List<WorfSyncStatus> s_syncingTables = [];
    private static PgReplicationConnection? s_applyconn;
    private static ApplyFunctions s_applyApi = ApplyFunctions.Heap;
    private static WorfRelation? s_lastInsertRel;
    private static int s_lastInsertRelCnt;
    private static bool s_useMultiInsert;
    private static long s_clockSkewReportedAt;
    private static ulong s_remoteCommitLsn;
    private static long s_remoteCommitTime;

    // A failed remote transaction that worf.exception_behaviour applies again, how, and whether the transaction being
    // applied is that one.
    private static ulong s_exceptionLsn;
    private static int s_exceptionLevel;
    private static bool s_exceptionMode;

    // The change being applied, for the exception log, or a null operation between changes.
    private static string? s_changeOperation;
    private static byte[]? s_changeMessage;
    private static string? s_changeQueuedMessage;

    // A message counter for the xact, for debugging. We don't send the remote change LSN with messages, so this aids
    // identification of which change causes an error.
    private static uint s_xactActionCounter;

    private static string? s_actionName;
    private static WorfRelation? s_errRel;
    private static bool s_isDdlOrDrop;
    private static uint s_remoteXid;
    private static PgErrorContext? s_errorContext;

    /// <summary>
    /// Gets how far ahead of the local clock a remote transaction's commit time can be for apply to wait for the clock
    /// to pass it, or -1 to never wait.
    /// </summary>
    [PgGucInt("worf.max_clock_skew", 1000, "Sets how long apply waits for the local clock to pass a remote commit time.",
        Minimum = -1, Maximum = int.MaxValue, Unit = PgGucUnit.Milliseconds, Context = PgGucContext.Sighup)]
    internal static partial int MaxClockSkew { get; }

    /// <summary>
    /// Gets what apply does with a remote transaction that fails.
    /// </summary>
    [PgGucEnum("worf.exception_behaviour", WorfExceptionBehaviour.Error,
        "Sets what apply does with a remote transaction that fails.", Context = PgGucContext.Sighup)]
    internal static partial WorfExceptionBehaviour ExceptionBehaviour { get; }

    // Set by the commit message while the remote transaction's local transaction runs.
    private static ulong? s_commitEndLsn;
    private static ulong s_lastReceived;

    private static ulong s_lastRecvpos;
    private static ulong s_lastWritepos;
    private static ulong s_lastFlushpos;
    private static long s_lastSendTime;

    /// <summary>
    /// Gets or sets the subscription this worker applies.
    /// </summary>
    internal static WorfSubscription? MySubscription { get; set; }

    /// <summary>
    /// Gets the node that wrote the remote transaction being applied.
    /// </summary>
    internal static uint WriterNodeId => s_writerNodeId;

    /// <summary>
    /// Gets or sets the slot of the worker whose apply state this process uses.
    /// </summary>
    internal static int MyApplyWorkerSlot { get; set; } = -1;

    /// <summary>
    /// Gets the apply state of this process's worker, as <c>MyApplyWorker</c> points to it.
    /// </summary>
    private static WorfApplyWorker MyApplyWorker => Worker.MyWorker.Worker.Apply;

    /// <summary>
    /// The apply worker's entry point.
    /// </summary>
    /// <param name="mainArg">The worker's slot.</param>
    [PgBackgroundWorker(EntryPoint = "worf_apply_main")]
    internal static void ApplyMain(nuint mainArg)
    {
        int slot = (int)mainArg;

        // Setup shmem.
        Worker.WorkerAttach(slot, WorfWorkerType.Apply);
        MyApplyWorkerSlot = slot;

        // Load correct apply API.
        if (Worf.UseSpi)
        {
            if (Conflict.ConflictResolver != WorfResolveOption.Error)
            {
                throw new PgException(PgSqlStates.InvalidParameterValue,
                    "worf.use_spi can only be used when worf.conflict_resolution is set to 'error'");
            }

            s_applyApi = ApplyFunctions.Spi;
        }

        // Setup synchronous commit according to the user's wishes
        Worker.SetConfigOption("synchronous_commit", Worf.SynchronousCommit ? "local" : "off");

        // Run as replica session replication role.
        Worker.SetConfigOption("session_replication_role", "replica", GucContext.PGC_SUSET);

        // Disable function body checks during replay. That's necessary because a) the creator of the function might
        // have had it disabled b) the function might be search_path dependant and we don't fix the contents of
        // functions.
        Worker.SetConfigOption("check_function_bodies", "off", GucContext.PGC_INTERNAL);

        // Load the subscription.
        uint subid = MyApplyWorker.SubId;
        WorfSubscription sub = PgBackgroundWorker.RunTransaction(() => WorfNode.GetSubscription(subid));
        MySubscription = sub;

        // A crashed worker's failed transaction is applied again under worf.exception_behaviour.
        s_exceptionLsn = MyApplyWorker.ExceptionLsn;
        s_exceptionLevel = MyApplyWorker.ExceptionLevel;

        PgLog.Log($"starting apply for subscription {sub.Name}");

        // Set apply delay if any.
        if (sub.ApplyDelay is PgInterval applyDelay)
        {
            s_applyDelay = IntervalToTimeoffset(applyDelay) / 1000;
        }

        // If the subscription isn't initialized yet, initialize it.
        bool catchingUp = Sync.SyncSubscription(sub);

        PgLog.Debug1($"connecting to provider {sub.Origin.Name}, dsn {sub.OriginIf.Dsn}");

        // Cache the queue relation id.
        // TODO: invalidation
        ulong originStartpos = PgBackgroundWorker.RunTransaction(() =>
        {
            s_queueRelid = Queue.GetQueueTableOid();

            ushort originid = PgReplicationOrigin.Find(sub.SlotName)
                ?? throw new PgException(PgSqlStates.UndefinedObject, $"replication origin \"{sub.SlotName}\" does not exist");
            PgLog.Debug2($"setting up replication origin {sub.SlotName} (oid {originid})");
            PgReplicationOrigin.SetupSession(originid);

            // Relayed transactions advance their relay origins instead, so start after the latest of them all.
            ulong startpos = PgReplicationOrigin.GetSessionProgress(false).Value;
            foreach ((ushort relay, _) in WorfNode.RelayOrigins(sub.SlotName))
            {
                startpos = Math.Max(startpos, PgReplicationOrigin.GetProgress(relay, false).Value);
            }

            return startpos;
        });

        // Start the replication.
        using PgReplicationConnection streamConn = Worf.ConnectReplica(sub.OriginIf.Dsn, sub.Name, null);
        string repsets = ReplicationSet.StringlistToIdentifierstr(sub.ReplicationSets);
        string origins = ReplicationSet.StringlistToIdentifierstr(sub.ForwardOrigins);

        // IDENTIFY_SYSTEM sets up some internal state on walsender so call it even if we don't (yet) want to use
        // any of the results.
        _ = PgBackgroundWorker.RunTransaction(() => Worf.IdentifySystem(streamConn));

        // The walsender of an apply worker that has just exited, as when the subscription's interface changes,
        // can hold the slot a moment longer. Wait for it, as the sync worker's cleanup does, rather than failing
        // and being restarted seconds later. A slot that stays busy fails below as before.
        long deadline = Worker.GetCurrentTimestamp() + 5000000L;
        while (!Worker.GotSigterm && Worker.GetCurrentTimestamp() < deadline &&
            PgBackgroundWorker.RunTransaction(() => Rpc.RemoteSlotActive(streamConn, sub.SlotName)))
        {
            _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(50));
        }

        Worf.StartReplication(streamConn, sub.SlotName, originStartpos, origins, repsets, null, sub.ForceTextTransfer);

        if (catchingUp)
        {
            Sync.FinishSubscriptionSync(sub);
        }

        ApplyWork(streamConn);

        // We should only get here if we received sigTERM
    }

    /// <summary>
    /// Apply main loop.
    /// </summary>
    /// <param name="streamConn">The streaming connection.</param>
    internal static void ApplyWork(PgReplicationConnection streamConn)
    {
        s_applyconn = streamConn;
        s_lastReceived = 0;

        // Init the MessageContext which we use for easier cleanup.
        fixed (byte* name = Worf.ToServerCString("MessageContext"))
        {
            NativeGlobals.MessageContext = NativeMethods.AllocSetContextCreateInternal(NativeGlobals.TopMemoryContext,
                (sbyte*)name, 0, 8 * 1024, 8 * 1024 * 1024);
        }

        using PgErrorContext errorContext = PgErrorContext.Push("");
        s_errorContext = errorContext;

        try
        {
            while (!Worker.GotSigterm)
            {
                PgInterrupts.Check();
                Worker.ProcessConfigReload();

                // Wait for data, the latch or the timeout.
                PgReplicationReceiveStatus status = streamConn.Receive(TimeSpan.FromMilliseconds(1000), out ReadOnlySpan<byte> copybuf);
                long feedbackAt = Stopwatch.GetTimestamp();

                while (status == PgReplicationReceiveStatus.Message)
                {
                    if (Worker.GotSigterm)
                    {
                        break;
                    }

                    HandleCopyData(copybuf.ToArray(), inTransaction: false);

                    // While apply is behind, the walsender's request for a reply waits behind the changes still to be
                    // applied, so report progress every second rather than only once caught up.
                    if (Stopwatch.GetElapsedTime(feedbackAt) >= TimeSpan.FromSeconds(1))
                    {
                        SendFeedback(streamConn, s_lastReceived, Worker.GetCurrentTimestamp(), false);
                        feedbackAt = Stopwatch.GetTimestamp();
                    }

                    status = streamConn.Receive(TimeSpan.Zero, out copybuf);
                }

                if (status == PgReplicationReceiveStatus.Ended)
                {
                    throw new PgException(PgSqlStates.InternalError, "data stream ended");
                }

                // confirm all writes at once
                SendFeedback(streamConn, s_lastReceived, Worker.GetCurrentTimestamp(), false);

                if (!s_inRemoteTransaction)
                {
                    ProcessSyncingTables(s_lastReceived);
                }
            }
        }
        catch (OperationCanceledException) when (Worker.GotSigterm)
        {
            // The worker was asked to stop in the middle of a remote transaction.
        }
        finally
        {
            s_errorContext = null;
        }
    }

    /// <summary>
    /// Execute an SQL command. This can be multiple multiple queries.
    /// </summary>
    /// <param name="cmdstr">The commands.</param>
    /// <param name="role">The role that ran them on the origin.</param>
    /// <param name="isTopLevel">Whether the commands may run as a top level statement.</param>
    /// <param name="withoutConcurrently">Whether commands that would run concurrently run in the transaction.</param>
    internal static void ExecuteSqlCommand(string cmdstr, string role, bool isTopLevel, bool withoutConcurrently = false)
    {
        sbyte* saveDebugQueryString = NativeGlobals.debug_query_string;

        MemoryContextData* oldcontext = Worf.MemoryContextSwitchTo(NativeGlobals.MessageContext);

        using PgErrorContext errcallback = PgErrorContext.Push($"during execution of queued SQL statement: {cmdstr}");

        sbyte* query = Worf.PStrDup(cmdstr);
        NativeGlobals.debug_query_string = query;

        List* commands = NativeMethods.pg_parse_query(query);

        _ = Worf.MemoryContextSwitchTo(oldcontext);

        // Do a limited amount of safety checking against CONCURRENTLY commands executed in situations where they
        // aren't allowed. The sender side should provide protection, but better be safe than sorry.
        isTopLevel = isTopLevel && commands is not null && commands->length == 1;

        for (int i = 0; commands is not null && i < commands->length; i++)
        {
            RawStmt* command = (RawStmt*)commands->elements[i].ptr_value;
            if (withoutConcurrently)
            {
                RemoveConcurrently(command->stmt);
            }

            // temporarily push snapshot for parse analysis/planning
            NativeMethods.PushActiveSnapshot(NativeMethods.GetTransactionSnapshot());

            oldcontext = Worf.MemoryContextSwitchTo(NativeGlobals.MessageContext);

            // Set the current role to the user that executed the command on the origin server.
            int saveNestlevel = NativeMethods.NewGUCNestLevel();
            Worker.SetConfigOption("role", role, GucContext.PGC_INTERNAL);

            CommandTag commandTag = NativeMethods.CreateCommandTag(command->stmt);

            List* querytreeList = NativeMethods.pg_analyze_and_rewrite_fixedparams(command, query, null, 0, null);

            List* plantreeList = NativeMethods.pg_plan_queries(querytreeList, query, 0, null);

            NativeMethods.PopActiveSnapshot();

            PortalData* portal;
            fixed (byte* name = Worf.ToServerCString(Worf.ExtensionName))
            {
                portal = NativeMethods.CreatePortal((sbyte*)name, true, true);
            }

            // Run the statement under the current resource owner, as SPI does. Dropping a portal hands the locks
            // its own owner took to the transaction, and once a statement took more than 15 that scans every lock
            // the transaction holds, so a transaction that runs many DDL statements would take quadratic time.
            NativeMethods.ResourceOwnerDelete(portal->resowner);
            portal->resowner = NativeGlobals.CurrentResourceOwner;

            NativeMethods.PortalDefineQuery(portal, null, query, commandTag, plantreeList, null);
            NativeMethods.PortalStart(portal, null, 0, null);

            _DestReceiver* receiver = NativeMethods.CreateDestReceiver(CommandDest.DestNone);

#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
            _ = NativeMethods.PortalRun(portal, NativeMethods.FETCH_ALL(), isTopLevel, true, receiver, receiver, null);
#else
            _ = NativeMethods.PortalRun(portal, NativeMethods.FETCH_ALL(), isTopLevel, receiver, receiver, null);
#endif
            receiver->rDestroy.Invoke(receiver);

            // Dropping the portal would release and delete its resource owner, so take the current one back, along
            // with the snapshot that a utility statement returning rows registers on it.
            if (portal->holdSnapshot is not null)
            {
                NativeMethods.UnregisterSnapshotFromOwner(portal->holdSnapshot, portal->resowner);
                portal->holdSnapshot = null;
            }

            portal->resowner = null;
            NativeMethods.PortalDrop(portal, false);

            NativeMethods.CommandCounterIncrement();

            // Restore the GUC variables we set above.
            NativeMethods.AtEOXact_GUC(true, saveNestlevel);

            _ = Worf.MemoryContextSwitchTo(oldcontext);
        }

        NativeGlobals.debug_query_string = saveDebugQueryString;
    }

    /// <summary>
    /// Calculates how long the apply worker waits before applying a transaction, so that it applies the delay after
    /// the transaction committed on the origin.
    /// </summary>
    /// <remarks>
    /// Timestamps count microseconds from 2000-01-01 UTC, so the wait doesn't depend on the local time zone or its
    /// daylight saving changes.
    /// </remarks>
    /// <param name="commitTime">When the transaction committed on the origin.</param>
    /// <param name="current">The current time.</param>
    /// <param name="applyDelay">The delay, in milliseconds.</param>
    /// <returns>The wait, in microseconds; zero when there's no need to wait.</returns>
    internal static long ApplyDelaySleep(long commitTime, long current, long applyDelay)
    {
        // ensure no weirdness due to clock drift
        if (current <= commitTime)
        {
            return 0;
        }

        current -= applyDelay * 1000;

        // Calculate total sleep time in microseconds. Guard against negative values which can occur if we've already
        // passed the target time (due to processing delays).
        return commitTime > current ? commitTime - current : 0;
    }

    /// <summary>
    /// Converts an interval to microseconds, with 30-day months and 24-hour days.
    /// </summary>
    /// <param name="interval">The interval.</param>
    /// <returns>The microseconds.</returns>
    internal static long IntervalToTimeoffset(PgInterval interval)
    {
        long span = interval.Microseconds;

        span += interval.Months * 30L * 86400000000L;
        span += interval.Days * 24L * 3600000000L;

        return span;
    }

    /// <summary>
    /// Check if given relation is in process of being synchronized.
    /// </summary>
    private static bool ShouldApplyChangesForRel(string nspname, string relname)
    {
        // A resynchronization that started since the statuses were read truncates the table and is committed by the
        // time the caller holds the table's lock, and its commit has asked for the statuses to be read again. Applying
        // the change before then would add a row that the resynchronization's copy also brings, so read them now
        // rather than at the end of the remote transaction.
        if (MyApplyWorker.SyncPending)
        {
            RereadUnsyncedTables(MyApplyWorker.SubId);
        }

        if (s_syncingTables.Count > 0)
        {
            ulong originLsn = Worf.ReploriginSessionOriginLsn;
            foreach (WorfSyncStatus sync in s_syncingTables)
            {
                if (sync.Nspname == nspname &&
                    sync.Relname == relname &&
                    sync.Status != Sync.SyncStatusReady &&
                    !(sync.Status == Sync.SyncStatusSyncdone && sync.Statuslsn <= originLsn))
                {
                    return false;
                }
            }
        }

        return true;
    }

    /// <summary>
    /// Prepare apply state details for errcontext or direct logging.
    /// </summary>
    private static string FormatActionDescription(string? actionName, WorfRelation? rel, bool isDdlOrDrop)
    {
        StringBuilder si = new("apply ");
        si.Append(actionName ?? "(unknown action)");

        if (rel is not null && !isDdlOrDrop)
        {
            si.Append(CultureInfo.InvariantCulture, $" from remote relation {rel.Nspname}.{rel.Relname}");
        }

        si.Append(CultureInfo.InvariantCulture,
            $" in commit before {Worf.FormatLsn(Worf.ReploriginSessionOriginLsn)}, xid {s_remoteXid} committed at {Worf.FromServer(NativeMethods.timestamptz_to_str(Worf.ReploriginSessionOriginTimestamp))} (action #{s_xactActionCounter})");

        if (Worf.ReploriginSessionOrigin != 0)
        {
            si.Append(CultureInfo.InvariantCulture, $" from node replorigin {Worf.ReploriginSessionOrigin}");
        }

        if (s_remoteOriginId != 0)
        {
            si.Append(CultureInfo.InvariantCulture,
                $" forwarded from commit {Worf.FormatLsn(s_remoteOriginLsn)} on node {s_remoteOriginId}");
        }

        return si.ToString();
    }

    /// <summary>
    /// Updates the error context line to the current action.
    /// </summary>
    private static void UpdateErrorContext()
    {
        if (s_errorContext is not null)
        {
            s_errorContext.Text = FormatActionDescription(s_actionName, s_errRel, s_isDdlOrDrop);
        }
    }

    private static void HandleBegin(StringInfoData* s)
    {
        s_xactActionCounter = 1;
        s_actionName = "BEGIN";

        ProtoNative.ReadBegin(s, out ulong commitLsn, out long commitTime, out s_remoteXid);

        Worf.ReploriginSessionOriginTimestamp = commitTime;
        Worf.ReploriginSessionOriginLsn = commitLsn;
        s_remoteOriginId = 0;
        s_writerNodeId = MySubscription!.Origin.Id;
        s_skipRemoteTransaction = false;
        s_remoteCommitLsn = commitLsn;
        s_remoteCommitTime = commitTime;
        s_changeOperation = null;
        s_exceptionMode = s_exceptionLsn != 0 && commitLsn == s_exceptionLsn &&
            ExceptionBehaviour is WorfExceptionBehaviour.Discard or WorfExceptionBehaviour.Transdiscard;
        UpdateErrorContext();

        // don't want the overhead otherwise
        if (s_applyDelay > 0)
        {
            long sleepUsec = ApplyDelaySleep(Worf.ReploriginSessionOriginTimestamp, Worker.GetCurrentTimestamp(), s_applyDelay);
            if (sleepUsec > 0)
            {
                // On Windows, the timer has lower precision (~15ms granularity) due to the default system timer
                // resolution. Add a small buffer to ensure we meet the requested delay despite timer imprecision.
                if (OperatingSystem.IsWindows())
                {
                    sleepUsec += 100000;  // 100ms buffer
                }

                NativeMethods.pg_usleep(sleepUsec);
            }
        }

        WaitForClock(commitTime);

        s_inRemoteTransaction = true;
    }

    /// <summary>
    /// Waits for the local clock to pass a remote transaction's commit time, so that local changes made after the
    /// transaction applies commit later than it did, as last_update_wins needs.
    /// </summary>
    /// <param name="commitTime">The remote commit time.</param>
    private static void WaitForClock(long commitTime)
    {
        long ahead = commitTime - Worker.GetCurrentTimestamp();
        int maxClockSkew = MaxClockSkew;
        if (maxClockSkew < 0)
        {
            return;
        }

        if (ahead > maxClockSkew * 1000L)
        {
            // Reported at most once a minute, so that skew hovering at the limit doesn't flood the log.
            long now = Worker.GetCurrentTimestamp();
            if (now - s_clockSkewReportedAt >= 60_000_000)
            {
                s_clockSkewReportedAt = now;
                PgLog.Warning(new PgDiagnostic(
                    $"remote commit time is {ahead / 1000} ms ahead of the local clock, more than worf.max_clock_skew")
                {
                    Hint = "Synchronize the nodes' clocks. Until then, local changes can lose to earlier remote ones.",
                });
            }

            return;
        }

        // Wait on the latch a second at most at a time, so that the worker stops when asked to and keeps the
        // provider's walsender from timing out.
        while (ahead >= 0)
        {
            if (Worker.GotSigterm)
            {
                throw new OperationCanceledException("The worf worker is stopping.");
            }

            _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(Math.Min((ahead / 1000) + 1, 1000)));
            PgInterrupts.Check();

            long now = Worker.GetCurrentTimestamp();
            if (s_applyconn is not null)
            {
                _ = SendFeedback(s_applyconn, s_lastReceived, now, false);
            }

            ahead = commitTime - now;
        }
    }

    /// <summary>
    /// Handle COMMIT message. Inside the remote transaction's local transaction this prepares its commit; the rest
    /// runs after the commit, in <see cref="FinishCommit"/>.
    /// </summary>
    private static void HandleCommit(StringInfoData* s, bool inTransaction)
    {
        s_actionName = "COMMIT";
        s_xactActionCounter++;
        UpdateErrorContext();
        TrackChange("COMMIT");

        ProtoNative.ReadCommit(s, out _, out ulong endLsn, out _);

        if (inTransaction)
        {
            MultiInsertFinish();

            s_applyApi.OnCommit();

            // We need to write end_lsn to the commit record. It's set once deferred triggers have run, just before the
            // record is written: an abort advances the origin too, and has to leave it at the remote commit position
            // that BEGIN set, from which the provider sends the transaction again.
            PgTransaction.RegisterCallback(PgTransactionEvent.PreCommit, () => Worf.ReploriginSessionOriginLsn = endLsn);

            s_commitEndLsn = endLsn;
            return;
        }

        FinishCommit(endLsn, false);
    }

    /// <summary>
    /// The part of COMMIT handling after the local transaction commits.
    /// </summary>
    private static void FinishCommit(ulong endLsn, bool committed)
    {
        if (committed)
        {
            // Track commit lsn
            _ = s_lsnMapping.AddLast((NativeGlobals.XactLastCommitEnd, endLsn));
        }

        // If the xact isn't from the immediate upstream, advance the slot of the node it originally came from so we
        // start replay of that node's change data at the right place.
        //
        // This is only necessary when we're streaming data from one peer (A) that in turn receives from other peers
        // (B, C), and we plan to later switch to replaying directly from B and/or C, no longer receiving forwarded
        // xacts from A.
        //
        // A failure can occur here if there's a cascading replication configuration where the direct and indirect
        // connections from X to Z use different replication sets so as not to conflict, and where Y and Z are on the
        // same PostgreSQL instance. In this case our attempt to advance the replication identifier here will ERROR
        // because it's already in use for the direct connection from X to Z. So don't do that.
        if (s_remoteOriginId != 0 && s_remoteOriginId != Worf.ReploriginSessionOrigin)
        {
            ulong xactLastCommitEnd = NativeGlobals.XactLastCommitEnd;
            ushort remoteOriginId = s_remoteOriginId;
            ulong remoteOriginLsn = s_remoteOriginLsn;
            PgLog.Debug3($"advancing origin oid {remoteOriginId} for forwarded row to {Worf.FormatLsn(xactLastCommitEnd)}");

            PgBackgroundWorker.RunTransaction(() =>
                PgReplicationOrigin.Advance(remoteOriginId, new PgLsn(remoteOriginLsn), new PgLsn(xactLastCommitEnd), false, false /* XXX ? */));
        }

        s_inRemoteTransaction = false;

        // Stop replay if we're doing limited replay and we've replayed up to the last record we're supposed to
        // process.
        WorfWorker me = Worker.MyWorker;
        ulong replayStopLsn = me.Worker.Apply.ReplayStopLsn;
        if (replayStopLsn != 0 && replayStopLsn <= endLsn)
        {
            PgLog.Log($"worf {(me.WorkerType == WorfWorkerType.Sync ? "sync" : "apply")} finished processing; replayed to {Worf.FormatLsn(endLsn)} of required {Worf.FormatLsn(replayStopLsn)}");

            // If this is sync worker, update syncing table state to done.
            if (me.WorkerType == WorfWorkerType.Sync)
            {
                PgBackgroundWorker.RunTransaction(() => Sync.SetTableSyncStatus(me.Worker.Apply.SubId,
                    Worker.NameStr(me.Worker.Sync.Nspname), Worker.NameStr(me.Worker.Sync.Relname), Sync.SyncStatusSyncdone, endLsn));
            }

            // Flush all writes so the latest position can be reported back to the sender.
            NativeMethods.XLogFlush(NativeMethods.GetXLogWriteRecPtr());

            // Disconnect.
            //
            // This needs to happen before the SyncWorkerFinish() call otherwise slot drop will fail.
            s_applyconn!.Dispose();

            // If this is sync worker, finish it.
            if (me.WorkerType == WorfWorkerType.Sync)
            {
                Sync.SyncWorkerFinish(Worker.NameStr(me.Worker.Sync.Nspname), Worker.NameStr(me.Worker.Sync.Relname));
            }

            // Stop gracefully
            Worker.ProcExit(0);
        }

        s_xactActionCounter = 0;
        s_remoteXid = 0;

        ProcessSyncingTables(endLsn);
    }

    /// <summary>
    /// Handle ORIGIN message.
    /// </summary>
    private static void HandleOrigin(StringInfoData* s)
    {
        string origin = ProtoNative.ReadOrigin(s, out s_remoteOriginLsn, out uint nodeId);
        s_remoteOriginId = PgReplicationOrigin.Find(origin) ?? 0;

        // The provider relays a transaction another node wrote. One this node wrote has come back, and is skipped.
        if (nodeId != 0)
        {
            s_writerNodeId = nodeId;
            s_skipRemoteTransaction = nodeId == MySubscription!.Target.Id;
        }

        UseWriterOrigin();
    }

    /// <summary>
    /// Commits the remote transaction under the origin for the node that wrote it, so that its commit timestamps
    /// record that node: the worker's own origin for its provider's transactions, or a relay origin for another
    /// node's.
    /// </summary>
    private static void UseWriterOrigin()
    {
        WorfSubscription sub = MySubscription!;
        uint writer = s_writerNodeId == sub.Origin.Id ? 0 : s_writerNodeId;
        if (writer == s_sessionWriterNodeId || s_skipRemoteTransaction)
        {
            return;
        }

        string name = writer == 0 ? sub.SlotName : WorfNode.RelayOriginName(sub.SlotName, writer);
        ushort origin = PgReplicationOrigin.Find(name) ?? PgReplicationOrigin.Create(name);

        // Keep the transaction's remote commit position and time across the change of origin.
        long commitTime = Worf.ReploriginSessionOriginTimestamp;
        ulong commitLsn = Worf.ReploriginSessionOriginLsn;
        PgReplicationOrigin.ResetSession();
        PgReplicationOrigin.SetupSession(origin);
        Worf.ReploriginSessionOriginTimestamp = commitTime;
        Worf.ReploriginSessionOriginLsn = commitLsn;
        s_sessionWriterNodeId = writer;
    }

    /// <summary>
    /// Handle RELATION message. Note we don't do validation against local schema here. The validation is posponed
    /// until first change for given relation comes.
    /// </summary>
    private static void HandleRelation(StringInfoData* s)
    {
        MultiInsertFinish();

        _ = ProtoNative.ReadRel(s);
    }

    private static void HandleInsert(StringInfoData* s, bool startedTx)
    {
        if (s_skipRemoteTransaction)
        {
            return;
        }

        NativeMethods.PushActiveSnapshot(NativeMethods.GetTransactionSnapshot());

        s_actionName = "INSERT";
        s_xactActionCounter++;
        UpdateErrorContext();
        TrackChange("INSERT");

        WorfRelation rel = ProtoNative.ReadInsert(s, PgLockMode.RowExclusive, out WorfTupleData newtup);
        s_errRel = rel;
        UpdateErrorContext();

        // If in list of relations which are being synchronized, skip.
        if (!ShouldApplyChangesForRel(rel.Nspname, rel.Relname))
        {
            RelCache.RelationClose(rel, PgLockMode.None);
            NativeMethods.PopActiveSnapshot();
            NativeMethods.CommandCounterIncrement();
            return;
        }

        // Handle multi_insert capabilities.
        if (s_useMultiInsert)
        {
            if (rel != s_lastInsertRel)
            {
                MultiInsertFinish();

                // Fall through to normal insert.
            }
            else
            {
                s_applyApi.MultiInsertAddTuple(rel, newtup);
                s_lastInsertRelCnt++;
                return;
            }
        }
        else if (Worf.BatchInserts &&
                 !s_exceptionMode &&
                 rel.Reloid != s_queueRelid &&
                 s_applyApi.CanMultiInsert(rel))
        {
            if (rel != s_lastInsertRel)
            {
                s_lastInsertRel = rel;
                s_lastInsertRelCnt = 0;
            }
            else if (s_lastInsertRelCnt++ >= MinMultiInsertTuples)
            {
                s_useMultiInsert = true;
                s_lastInsertRelCnt = 0;
            }
        }

        // Normal insert.
        s_applyApi.DoInsert(rel, newtup);

        // if INSERT was into our queue, process the message.
        if (rel.Reloid == s_queueRelid)
        {
            RelationData* relation = rel.Rel;
            LockRelId lockid = relation->rd_lockInfo.lockRelId;

            MultiInsertFinish();

            newtup.ConvertValues(relation->rd_att);
            HeapTupleData* ht;
            fixed (ulong* values = newtup.Values)
            fixed (bool* nulls = newtup.Nulls)
            {
                ht = NativeMethods.heap_form_tuple(relation->rd_att, values, nulls);
            }

            NativeMethods.LockRelationIdForSession(&lockid, (int)PgLockMode.RowExclusive);
            try
            {
                RelCache.RelationClose(rel, PgLockMode.None);

                NativeMethods.PopActiveSnapshot();
                NativeMethods.CommandCounterIncrement();

                s_applyApi.OnCommit();

                HandleQueuedMessage(ht, startedTx);

                NativeMethods.heap_freetuple(ht);

                RelationData* qrel = NativeMethods.table_open(s_queueRelid, (int)PgLockMode.RowExclusive);
                NativeMethods.table_close(qrel, (int)PgLockMode.None);
            }
            finally
            {
                // A session lock outlives a subtransaction that worf.exception_behaviour rolls back.
                NativeMethods.UnlockRelationIdForSession(&lockid, (int)PgLockMode.RowExclusive);
            }

            s_applyApi.OnBegin();
        }
        else
        {
            RelCache.RelationClose(rel, PgLockMode.None);

            NativeMethods.PopActiveSnapshot();
            NativeMethods.CommandCounterIncrement();
        }
    }

    private static void MultiInsertFinish()
    {
        if (s_useMultiInsert && s_lastInsertRelCnt != 0)
        {
            string? oldAction = s_actionName;
            WorfRelation? oldRel = s_errRel;
            string? oldOperation = s_changeOperation;
            s_actionName = "multi INSERT";
            s_errRel = s_lastInsertRel;
            UpdateErrorContext();

            // A failure belongs to the batch, whichever message made apply write it.
            s_changeOperation = s_actionName;

            s_applyApi.MultiInsertFinish(s_lastInsertRel!);
            RelCache.RelationClose(s_lastInsertRel!, PgLockMode.None);
            s_useMultiInsert = false;
            s_lastInsertRel = null;
            s_lastInsertRelCnt = 0;

            s_errRel = oldRel;
            s_actionName = oldAction;
            s_changeOperation = oldOperation;
            UpdateErrorContext();
        }
    }

    private static void HandleUpdate(StringInfoData* s)
    {
        if (s_skipRemoteTransaction)
        {
            return;
        }

        s_actionName = "UPDATE";
        s_xactActionCounter++;
        UpdateErrorContext();
        TrackChange("UPDATE");

        MultiInsertFinish();

        NativeMethods.PushActiveSnapshot(NativeMethods.GetTransactionSnapshot());

        WorfRelation rel = ProtoNative.ReadUpdate(s, PgLockMode.RowExclusive, out bool hasoldtup,
            out WorfTupleData? oldtup, out WorfTupleData newtup);
        s_errRel = rel;
        UpdateErrorContext();

        // If in list of relations which are being synchronized, skip.
        if (!ShouldApplyChangesForRel(rel.Nspname, rel.Relname))
        {
            RelCache.RelationClose(rel, PgLockMode.None);
            NativeMethods.PopActiveSnapshot();
            NativeMethods.CommandCounterIncrement();
            return;
        }

        s_applyApi.DoUpdate(rel, hasoldtup ? oldtup! : newtup, newtup);

        RelCache.RelationClose(rel, PgLockMode.None);

        NativeMethods.PopActiveSnapshot();
        NativeMethods.CommandCounterIncrement();
    }

    private static void HandleDelete(StringInfoData* s)
    {
        if (s_skipRemoteTransaction)
        {
            return;
        }

        s_actionName = null;
        s_errRel = null;
        s_isDdlOrDrop = false;
        s_xactActionCounter++;
        UpdateErrorContext();
        TrackChange("DELETE");

        MultiInsertFinish();

        NativeMethods.PushActiveSnapshot(NativeMethods.GetTransactionSnapshot());

        WorfRelation rel = ProtoNative.ReadDelete(s, PgLockMode.RowExclusive, out WorfTupleData oldtup);
        s_errRel = rel;
        UpdateErrorContext();

        // If in list of relations which are being synchronized, skip.
        if (!ShouldApplyChangesForRel(rel.Nspname, rel.Relname))
        {
            RelCache.RelationClose(rel, PgLockMode.None);
            NativeMethods.PopActiveSnapshot();
            NativeMethods.CommandCounterIncrement();
            return;
        }

        s_applyApi.DoDelete(rel, oldtup);

        RelCache.RelationClose(rel, PgLockMode.None);

        NativeMethods.PopActiveSnapshot();
        NativeMethods.CommandCounterIncrement();
    }

    private static void HandleStartup(StringInfoData* s)
    {
        int msgver = NativeMethods.pq_getmsgbyte(s);
        if (msgver != 1)
        {
            throw new PgException(PgSqlStates.InternalError, $"Expected startup message version 1, but got {msgver}");
        }

        // The startup message consists of null-terminated strings as key/value pairs. The first entry is always the
        // format identifier.
        do
        {
            string k = Worf.FromServer(NativeMethods.pq_getmsgstring(s));
            if (k.Length == 0)
            {
                throw new PgException(PgSqlStates.ProtocolViolation, "invalid startup message: key has zero length");
            }

            if (Worf.UnreadLength(s) == 0)
            {
                throw new PgException(PgSqlStates.ProtocolViolation, $"invalid startup message: key '{k}' has no following value");
            }

            // It's OK to have a zero length value
            string v = Worf.FromServer(NativeMethods.pq_getmsgstring(s));

            HandleStartupParam(k, v);
        }
        while (Worf.UnreadLength(s) != 0);
    }

    private static bool ParseBoolParam(string key, string value)
    {
        bool result;
        fixed (byte* cvalue = Worf.ToServerCString(value))
        {
            if (!NativeMethods.parse_bool((sbyte*)cvalue, &result))
            {
                throw new PgException(PgSqlStates.InvalidParameterValue, $"couldn't parse value '{value}' for key '{key}' as boolean");
            }
        }

        return result;
    }

    private static void HandleStartupParam(string key, string value)
    {
        PgLog.Debug2($"apply got worf startup msg param  {key}={value}");

        if (key == "pg_version")
        {
            PgLog.Debug1($"upstream Pg version is {value}");
        }

        if (key == "encoding")
        {
            int encoding = OutputConfig.CharToEncoding(value);

            if (encoding != NativeMethods.GetDatabaseEncoding())
            {
                throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState,
                    $"expected encoding={Worf.DatabaseEncodingName} from upstream but got {value}");
            }
        }

        if (key == "forward_changeset_origins")
        {
            bool fwd = ParseBoolParam(key, value);

            // FIXME: Store this somewhere
            PgLog.Debug1($"changeset origin forwarding enabled: {(fwd ? "t" : "f")}");
        }

        // We just ignore a bunch of parameters here because we specify what we require when we send our params to
        // the upstream. It's required to ERROR if it can't match what we asked for.
    }

    private static (string Schemaname, string Relname) ParseRelationMessage(string message)
    {
        string? nspname = null;
        string? relname = null;

        using JsonDocument document = JsonDocument.Parse(message);
        JsonElement root = document.RootElement;

        // Parse and validate the json message.
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new PgException(PgSqlStates.InternalError, "malformed message in queued message tuple: root is not object");
        }

        foreach (JsonProperty property in root.EnumerateObject())
        {
            bool isSchema;
            if ("schema_name".StartsWith(property.Name, StringComparison.Ordinal))
            {
                isSchema = true;
            }
            else if ("table_name".StartsWith(property.Name, StringComparison.Ordinal))
            {
                isSchema = false;
            }
            else
            {
                throw new PgException(PgSqlStates.InternalError, $"unexpected key: {property.Name}");
            }

            if (property.Value.ValueKind != JsonValueKind.String)
            {
                throw new PgException(PgSqlStates.InternalError, $"unexpected type for key '{property.Name}': {JsonbType(property.Value)}");
            }

            if (isSchema)
            {
                nspname = property.Value.GetString();
            }
            else
            {
                relname = property.Value.GetString();
            }
        }

        // Check if we got both schema and table names.
        if (nspname is null)
        {
            throw new PgException(PgSqlStates.InternalError, "missing schema_name in relation message");
        }

        if (relname is null)
        {
            throw new PgException(PgSqlStates.InternalError, "missing table_name in relation message");
        }

        return (nspname, relname);
    }

    /// <summary>
    /// Handle TRUNCATE message comming via queue table.
    /// </summary>
    private static void HandleTruncate(QueuedMessage queuedMessage)
    {
        // If table doesn't exist locally, it can't be subscribed.
        //
        // TODO: should we error here?
        (string schemaname, string relname) = ParseRelationMessage(queuedMessage.Message);

        // If in list of relations which are being synchronized, skip.
        if (!ShouldApplyChangesForRel(schemaname, relname))
        {
            return;
        }

        Sync.TruncateTable(schemaname, relname);
    }

    /// <summary>
    /// Handle TABLESYNC message comming via queue table.
    /// </summary>
    private static void HandleTableSync(QueuedMessage queuedMessage)
    {
        (string schemaname, string relname) = ParseRelationMessage(queuedMessage.Message);
        uint subid = MyApplyWorker.SubId;

        WorfSyncStatus? oldsync = Sync.GetTableSyncStatus(subid, schemaname, relname, true);

        if (oldsync is not null)
        {
            PgLog.Info($"table sync came from queue for table {schemaname}.{relname} which already being synchronized, skipping");

            return;
        }

        Sync.CreateLocalSyncStatus(new WorfSyncStatus
        {
            Kind = Sync.SyncKindDataOnly,
            Subid = subid,
            Status = Sync.SyncStatusInit,
            Nspname = schemaname,
            Relname = relname,
        });

        SetSyncPending(true);
    }

    /// <summary>
    /// Handle SEQUENCE message comming via queue table.
    /// </summary>
    private static void HandleSequence(QueuedMessage queuedMessage)
    {
        string? nspname = null;
        string? relname = null;
        string? lastValueRaw = null;

        using JsonDocument document = JsonDocument.Parse(queuedMessage.Message);
        JsonElement root = document.RootElement;

        // Parse and validate the json message.
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new PgException(PgSqlStates.InternalError, "malformed message in queued message tuple: root is not object");
        }

        foreach (JsonProperty property in root.EnumerateObject())
        {
            int target;
            if ("schema_name".StartsWith(property.Name, StringComparison.Ordinal))
            {
                target = 0;
            }
            else if ("sequence_name".StartsWith(property.Name, StringComparison.Ordinal))
            {
                target = 1;
            }
            else if ("last_value".StartsWith(property.Name, StringComparison.Ordinal))
            {
                target = 2;
            }
            else
            {
                throw new PgException(PgSqlStates.InternalError, $"unexpected key: {property.Name}");
            }

            if (property.Value.ValueKind != JsonValueKind.String)
            {
                throw new PgException(PgSqlStates.InternalError, $"unexpected type for key '{property.Name}': {JsonbType(property.Value)}");
            }

            string? value = property.Value.GetString();
            switch (target)
            {
                case 0:
                    nspname = value;
                    break;
                case 1:
                    relname = value;
                    break;
                default:
                    lastValueRaw = value;
                    break;
            }
        }

        // Check if we got both schema and table names.
        if (nspname is null)
        {
            throw new PgException(PgSqlStates.InternalError, "missing schema_name in sequence message");
        }

        if (relname is null)
        {
            throw new PgException(PgSqlStates.InternalError, "missing table_name in sequence message");
        }

        if (lastValueRaw is null)
        {
            throw new PgException(PgSqlStates.InternalError, "missing last_value in sequence message");
        }

        uint nspoid;
        uint reloid;
        fixed (byte* cnspname = Worf.ToServerCString(nspname))
        fixed (byte* crelname = Worf.ToServerCString(relname))
        {
            nspoid = NativeMethods.get_namespace_oid((sbyte*)cnspname, false);
            reloid = NativeMethods.get_relname_relid((sbyte*)crelname, nspoid);
        }

        long lastValue = ParseInt64Prefix(lastValueRaw);

        _ = Spi.Execute("SELECT pg_catalog.setval($1, $2)", SpiParameter.Create(reloid), SpiParameter.Create(lastValue));
    }

    /// <summary>
    /// Handle SQL message comming via queue table.
    /// </summary>
    private static void HandleSql(QueuedMessage queuedMessage, bool txJustStarted)
    {
        using JsonDocument document = JsonDocument.Parse(queuedMessage.Message);
        JsonElement root = document.RootElement;

        // Validate the json and extract the SQL string from it.
        if (root.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
        {
            throw new PgException(PgSqlStates.InternalError, "malformed message in queued message tuple: root is not scalar");
        }

        if (root.ValueKind != JsonValueKind.String)
        {
            throw new PgException(PgSqlStates.InternalError,
                $"malformed message in queued message tuple, expected value type 1 got {JsonbType(root)}");
        }

        string sql = root.GetString()!;

        // Run the extracted SQL.
        ExecuteSqlCommand(sql, queuedMessage.Role, txJustStarted);
    }

    /// <summary>
    /// Runs DDL that worf.enable_ddl_replication queued, with the search path it ran with.
    /// </summary>
    private static void HandleDdl(QueuedMessage queuedMessage, bool txJustStarted)
    {
        using JsonDocument document = JsonDocument.Parse(queuedMessage.Message);
        JsonElement root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object ||
            !root.TryGetProperty("search_path", out JsonElement searchPath) || searchPath.ValueKind != JsonValueKind.String ||
            !root.TryGetProperty("command", out JsonElement command) || command.ValueKind != JsonValueKind.String)
        {
            throw new PgException(PgSqlStates.InternalError, "malformed message in queued message tuple: expected search_path and command");
        }

        int saveNestlevel = NativeMethods.NewGUCNestLevel();
        fixed (byte* name = Worf.ToServerCString("search_path"))
        fixed (byte* value = Worf.ToServerCString(searchPath.GetString()!))
        {
            _ = NativeMethods.set_config_option((sbyte*)name, (sbyte*)value, GucContext.PGC_USERSET, GucSource.PGC_S_SESSION,
                GucAction.GUC_ACTION_SAVE, true, 0, false);
        }

        // A statement that runs concurrently commits along the way, which apply's transaction can't, so it runs
        // in that transaction instead.
        ExecuteSqlCommand(command.GetString()!, queuedMessage.Role, false, true);

        NativeMethods.AtEOXact_GUC(true, saveNestlevel);
    }

    /// <summary>
    /// Makes a statement that would build or drop an index or detach a partition concurrently do it in its
    /// transaction.
    /// </summary>
    private static void RemoveConcurrently(Node* stmt)
    {
        switch (stmt->type)
        {
            case NodeTag.T_IndexStmt:
                ((IndexStmt*)stmt)->concurrent = false;
                break;

            case NodeTag.T_DropStmt:
                ((DropStmt*)stmt)->concurrent = false;
                break;

            case NodeTag.T_AlterTableStmt:
                List* cmds = ((AlterTableStmt*)stmt)->cmds;
                for (int i = 0; cmds is not null && i < cmds->length; i++)
                {
                    AlterTableCmd* cmd = (AlterTableCmd*)cmds->elements[i].ptr_value;
                    if (cmd->subtype == AlterTableType.AT_DetachPartition)
                    {
                        ((PartitionCmd*)cmd->def)->concurrent = false;
                    }
                }

                break;

            default:
                break;
        }
    }

    /// <summary>
    /// Handles messages comming from the queue.
    /// </summary>
    private static void HandleQueuedMessage(HeapTupleData* msgtup, bool txJustStarted)
    {
        string? oldActionName = s_actionName;
        s_isDdlOrDrop = true;

        QueuedMessage queuedMessage = Queue.QueuedMessageFromTuple(msgtup);
        TrackChange(null);
        s_changeQueuedMessage = queuedMessage.Message;

        switch (queuedMessage.MessageType)
        {
            case Queue.QueueCommandTypeSql:
                s_actionName = "QUEUED_SQL";
                s_changeOperation = s_actionName;
                UpdateErrorContext();
                HandleSql(queuedMessage, txJustStarted);
                break;

            case Queue.QueueCommandTypeTruncate:
                s_actionName = "QUEUED_TRUNCATE";
                s_changeOperation = s_actionName;
                UpdateErrorContext();
                HandleTruncate(queuedMessage);
                break;

            case Queue.QueueCommandTypeTablesync:
                s_actionName = "QUEUED_TABLESYNC";
                s_changeOperation = s_actionName;
                UpdateErrorContext();
                HandleTableSync(queuedMessage);
                break;

            case Queue.QueueCommandTypeSequence:
                s_actionName = "QUEUED_SEQUENCE";
                s_changeOperation = s_actionName;
                UpdateErrorContext();
                HandleSequence(queuedMessage);
                break;

            case Queue.QueueCommandTypeDdl:
                s_actionName = "QUEUED_DDL";
                s_changeOperation = s_actionName;
                UpdateErrorContext();
                HandleDdl(queuedMessage, txJustStarted);
                break;

            default:
                throw new PgException(PgSqlStates.InternalError, $"unknown message type '{queuedMessage.MessageType}'");
        }

        s_actionName = oldActionName;
        s_isDdlOrDrop = false;
        UpdateErrorContext();
    }

    /// <summary>
    /// Handles one CopyData message from the stream.
    /// </summary>
    private static void HandleCopyData(byte[] copybuf, bool inTransaction)
    {
        fixed (byte* data = copybuf)
        {
            // We're using a StringInfo to wrap existing data here, as a cursor.
            StringInfoData s;
            s.data = (sbyte*)data;
            s.len = copybuf.Length;
            s.maxlen = -1;
            s.cursor = 0;

            int c = NativeMethods.pq_getmsgbyte(&s);

            if (c == 'w')
            {
                ulong startLsn = unchecked((ulong)NativeMethods.pq_getmsgint64(&s));
                ulong endLsn = unchecked((ulong)NativeMethods.pq_getmsgint64(&s));
                _ = NativeMethods.pq_getmsgint64(&s); // sendTime

                if (s_lastReceived < startLsn)
                {
                    s_lastReceived = startLsn;
                }

                if (s_lastReceived < endLsn)
                {
                    s_lastReceived = endLsn;
                }

                // The replication message is copied so it outlives this buffer when it starts a transaction.
                ReplicationHandler(copybuf.AsSpan(s.cursor).ToArray(), inTransaction);
            }
            else if (c == 'k')
            {
                ulong endpos = unchecked((ulong)NativeMethods.pq_getmsgint64(&s));
                _ = NativeMethods.pq_getmsgint64(&s); // timestamp
                bool replyRequested = NativeMethods.pq_getmsgbyte(&s) != 0;

                SendFeedback(s_applyconn!, endpos, Worker.GetCurrentTimestamp(), replyRequested);

                if (s_lastReceived < endpos)
                {
                    s_lastReceived = endpos;
                }

                // For sync workers in catchup mode, check if we've reached the stop position based on the keepalive.
                // This is needed because table-specific replication slots may not receive any actual commits if no
                // changes are made to the table, but keepalives tell us the provider's current position.
                WorfWorker me = Worker.MyWorker;
                if (!inTransaction &&
                    me.WorkerType == WorfWorkerType.Sync &&
                    me.Worker.Apply.ReplayStopLsn != 0 &&
                    me.Worker.Apply.ReplayStopLsn <= endpos)
                {
                    PgLog.Log($"worf sync finished processing via keepalive; provider at {Worf.FormatLsn(endpos)}, required {Worf.FormatLsn(me.Worker.Apply.ReplayStopLsn)}");

                    PgBackgroundWorker.RunTransaction(() => Sync.SetTableSyncStatus(me.Worker.Apply.SubId,
                        Worker.NameStr(me.Worker.Sync.Nspname), Worker.NameStr(me.Worker.Sync.Relname), Sync.SyncStatusSyncdone, endpos));

                    NativeMethods.XLogFlush(NativeMethods.GetXLogWriteRecPtr());
                    s_applyconn!.Dispose();
                    Sync.SyncWorkerFinish(Worker.NameStr(me.Worker.Sync.Nspname), Worker.NameStr(me.Worker.Sync.Relname));
                    Worker.ProcExit(0);
                }
            }

            // other message types are purposefully ignored
        }
    }

    private static void ReplicationHandler(byte[] message, bool inTransaction)
    {
        fixed (byte* data = message)
        {
            StringInfoData s;
            s.data = (sbyte*)data;
            s.len = message.Length;
            s.maxlen = -1;
            s.cursor = 0;

            char action = (char)NativeMethods.pq_getmsgbyte(&s);

            s_actionName = null;
            s_errRel = null;
            s_isDdlOrDrop = false;

            switch (action)
            {
                // BEGIN
                case 'B':
                    HandleBegin(&s);
                    break;

                // COMMIT
                case 'C':
                    HandleCommit(&s, inTransaction);
                    break;

                // ORIGIN
                case 'O':
                    // ORIGIN message can only come inside remote transaction and before any actual writes.
                    if (!s_inRemoteTransaction || inTransaction)
                    {
                        throw new PgException(PgSqlStates.InternalError, "ORIGIN message sent out of order");
                    }

                    // We have to start transaction here so that we can work with origins.
                    RunRemoteTransaction(message);
                    break;

                // RELATION
                case 'R':
                    HandleRelation(&s);
                    break;

                // INSERT, UPDATE, DELETE
                case 'I':
                case 'U':
                case 'D':
                    s_changeMessage = message;
                    if (!inTransaction)
                    {
                        RunRemoteTransaction(message);
                    }
                    else if (s_exceptionMode)
                    {
                        ApplyChange(message, false);
                    }
                    else
                    {
                        DispatchChange(action, &s, false);
                    }

                    break;

                // STARTUP MESSAGE
                case 'S':
                    HandleStartup(&s);
                    break;

                default:
                    throw new PgException(PgSqlStates.InternalError, $"unknown action of type {action}");
            }
        }
    }

    private static void DispatchChange(char action, StringInfoData* s, bool startedTx)
    {
        switch (action)
        {
            case 'O':
                HandleOrigin(s);
                break;
            case 'I':
                HandleInsert(s, startedTx);
                break;
            case 'U':
                HandleUpdate(s);
                break;
            default:
                HandleDelete(s);
                break;
        }

        s_changeOperation = null;
    }

    private static void DispatchChangeMessage(byte[] message, bool startedTx)
    {
        fixed (byte* data = message)
        {
            StringInfoData s;
            s.data = (sbyte*)data;
            s.len = message.Length;
            s.maxlen = -1;
            s.cursor = 0;

            char action = (char)NativeMethods.pq_getmsgbyte(&s);
            DispatchChange(action, &s, startedTx);
        }
    }

    /// <summary>
    /// Applies a change message. When <c>worf.exception_behaviour</c> discards changes, a change that fails is rolled
    /// back on its own, recorded and skipped.
    /// </summary>
    private static void ApplyChange(byte[] message, bool startedTx)
    {
        if (!s_exceptionMode || s_exceptionLevel != ExceptionByChange)
        {
            DispatchChangeMessage(message, startedTx);
            return;
        }

        try
        {
            PgTransaction.RunInSubtransaction(() =>
            {
                DispatchChangeMessage(message, startedTx);

                // The change's writers can't outlive its subtransaction.
                s_applyApi.OnCommit();
            }, PgSubtransactionMode.Atomic);
        }
        catch (PgException error) when (!IsTransient(error))
        {
            ForgetFailedChange();
            ExceptionLog.Record(FailedChange(), WorfExceptionBehaviour.Discard, error);
        }
    }

    /// <summary>
    /// Checks whether an error says nothing about the change's data, as a deadlock, a serialization failure, a lock or
    /// statement timeout, a cancellation, a shutdown, running out of resources, a lost connection or an I/O error do,
    /// so applying the transaction again can get past it.
    /// </summary>
    private static bool IsTransient(PgException error)
    {
        string state = error.SqlState;
        return state is "40000" or "40001" or "40003" or "40P01" or "55P03" or "58030" ||
            state.StartsWith("08", StringComparison.Ordinal) || state.StartsWith("53", StringComparison.Ordinal) ||
            state.StartsWith("57", StringComparison.Ordinal);
    }

    /// <summary>
    /// Forgets the relations, writers and batched inserts that a rolled back subtransaction opened, and released.
    /// </summary>
    private static void ForgetFailedChange()
    {
        s_applyApi.OnBegin();
        RelCache.ForgetOpenRelations();
        s_useMultiInsert = false;
        s_lastInsertRel = null;
        s_lastInsertRelCnt = 0;
    }

    private static void TrackChange(string? operation)
    {
        s_changeOperation = operation;
        s_changeQueuedMessage = null;
    }

    private static WorfFailedChange FailedChange() => new()
    {
        Subscription = MySubscription!,
        WriterNodeId = s_writerNodeId,
        RemoteXid = s_remoteXid,
        CommitTime = s_remoteCommitTime,
        CommitLsn = s_remoteCommitLsn,
        ActionCounter = s_xactActionCounter,
        Operation = s_changeOperation,
        Relation = s_errRel,
        Message = s_changeOperation is "INSERT" or "UPDATE" or "DELETE" ? s_changeMessage : null,
        QueuedMessage = s_changeQueuedMessage,
    };

    /// <summary>
    /// Handles a remote transaction whose change failed, after its local transaction rolled back, as
    /// <c>worf.exception_behaviour</c> selects. The error then stops the worker.
    /// </summary>
    private static void OnRemoteTransactionError(PgException error)
    {
        // An error that applying the transaction again can get past is retried, as without worf.exception_behaviour.
        if (s_changeOperation is null || IsTransient(error) || Worker.MyWorker.WorkerType != WorfWorkerType.Apply)
        {
            return;
        }

        string lsn = Worf.FormatLsn(s_remoteCommitLsn);
        switch (ExceptionBehaviour)
        {
            case WorfExceptionBehaviour.SubDisable:
                // The rollback released the relations the transaction opened.
                RelCache.ForgetOpenRelations();
                WorfFailedChange change = FailedChange();
                PgBackgroundWorker.RunTransaction(() =>
                {
                    ExceptionLog.Record(change, WorfExceptionBehaviour.SubDisable, error);
                    WorfSubscription sub = WorfNode.GetSubscription(change.Subscription.Id);
                    sub.Enabled = false;
                    WorfNode.AlterSubscription(sub);
                });
                PgLog.Log($"disabling subscription {change.Subscription.Name} after its remote transaction at {lsn} failed");
                break;

            case WorfExceptionBehaviour.Discard:
            case WorfExceptionBehaviour.Transdiscard:
                // A transaction whose commit fails, as on a deferred constraint, or that fails again change by change
                // is skipped as a whole next time. One whose commit fails even then, outside its subtransaction, is
                // recorded now and not applied at all. One that fails when it isn't applied stops the worker until
                // the error is fixed.
                int level = s_exceptionMode ? s_exceptionLevel + 1
                    : ExceptionBehaviour == WorfExceptionBehaviour.Transdiscard || s_changeOperation == "COMMIT"
                        ? ExceptionByTransaction
                        : ExceptionByChange;
                if (level > ExceptionWithoutApplying)
                {
                    break;
                }

                if (level == ExceptionWithoutApplying)
                {
                    RelCache.ForgetOpenRelations();
                    WorfFailedChange failed = FailedChange();
                    PgBackgroundWorker.RunTransaction(() =>
                        ExceptionLog.Record(failed, WorfExceptionBehaviour.Transdiscard, error));
                }

                SetExceptionMarker(s_remoteCommitLsn, level);
                PgLog.Log($"applying the remote transaction at {lsn} under worf.exception_behaviour once the worker restarts");
                break;

            default:
                break;
        }
    }

    private static void SetExceptionMarker(ulong lsn, int level)
    {
        using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive();
        WorfWorker worker = guard[Worker.MyWorfWorker];
        worker.Worker.Apply.ExceptionLsn = lsn;
        worker.Worker.Apply.ExceptionLevel = level;
        worker.Worker.Apply.ExceptionRestartsAtOnce = lsn != 0;
        guard[Worker.MyWorfWorker] = worker;
    }

    /// <summary>
    /// Moves the origin back to the failed remote transaction's commit position, from which the provider sends the
    /// transaction again. An abort advances the origin as a commit does, and one after the end position was set for
    /// the commit, as when the commit fails a serialization check, would skip the transaction.
    /// </summary>
    private static void RewindOrigin()
    {
        Worf.ReploriginSessionOriginLsn = s_remoteCommitLsn;
        ushort origin = PgReplicationOrigin.SessionOrigin;
        if (origin == 0)
        {
            return;
        }

        // Only an origin no session uses can move back. The worker stops after this, so it stays released.
        PgReplicationOrigin.ResetSession();
        ulong lsn = s_remoteCommitLsn;
        PgBackgroundWorker.RunTransaction(() => PgReplicationOrigin.Advance(origin, new PgLsn(lsn), PgLsn.Invalid, true, true));
    }

    /// <summary>
    /// Applies the rest of a remote transaction in one local transaction, starting with the message that needs it,
    /// until its COMMIT, then finishes the commit.
    /// </summary>
    private static void RunRemoteTransaction(byte[] firstMessage)
    {
        s_commitEndLsn = null;

        try
        {
            PgBackgroundWorker.RunTransaction(() =>
            {
                // The changes are applied in MessageContext, which is reset after each batch of messages so that a
                // large transaction doesn't keep every row's memory until it commits.
                MemoryContextData* oldcontext = Worf.MemoryContextSwitchTo(NativeGlobals.MessageContext);
                try
                {
                    ApplyRemoteTransaction(firstMessage);
                }
                finally
                {
                    _ = Worf.MemoryContextSwitchTo(oldcontext);
                }
            });
        }
        catch (PgException error)
        {
            RewindOrigin();
            OnRemoteTransactionError(error);
            throw;
        }

        if (s_exceptionMode)
        {
            s_exceptionMode = false;
            s_exceptionLsn = 0;
            SetExceptionMarker(0, 0);
        }

        ulong endLsn = s_commitEndLsn!.Value;
        s_commitEndLsn = null;
        FinishCommit(endLsn, true);
    }

    private static void ApplyRemoteTransaction(byte[] firstMessage)
    {
        // worf doesn't have "statements" as such, so we'll report one statement per applied transaction. The
        // transaction's start sets the transaction timestamp used by current_timestamp.
        s_applyApi.OnBegin();

        // An ORIGIN message names the node that wrote the transaction first. Its change of origin stays out of the
        // subtransactions that worf.exception_behaviour rolls back.
        bool origin = firstMessage[0] == 'O';
        if (origin)
        {
            DispatchChangeMessage(firstMessage, true);
        }
        else
        {
            UseWriterOrigin();
        }

        byte[]? firstChange = origin ? null : firstMessage;
        if (!s_exceptionMode)
        {
            ApplyChanges(firstChange);
            return;
        }

        if (s_exceptionLevel == ExceptionByChange)
        {
            ApplyChanges(firstChange);
            return;
        }

        // A transaction whose commit failed however it was applied was recorded then, and is skipped. Its commit
        // still advances the origin past it.
        if (s_exceptionLevel == ExceptionWithoutApplying)
        {
            s_skipRemoteTransaction = true;
            _ = NativeMethods.GetTopTransactionId();
            ApplyChanges(firstChange);
            return;
        }

        // A failure rolls the whole transaction back, and the rest of it is skipped.
        try
        {
            PgTransaction.RunInSubtransaction(() =>
            {
                ApplyChanges(firstChange);

                // Deferred constraints are checked here, where a failure rolls back with the rest.
                _ = Spi.Execute("SET CONSTRAINTS ALL IMMEDIATE");
            }, PgSubtransactionMode.Atomic);
        }
        catch (PgException error) when (!IsTransient(error))
        {
            ForgetFailedChange();
            ExceptionLog.Record(FailedChange(), WorfExceptionBehaviour.Transdiscard, error);
            s_skipRemoteTransaction = true;
            if (s_commitEndLsn is null)
            {
                ApplyChanges(null);
            }
        }
    }

    /// <summary>
    /// Applies the remote transaction's changes from the given one, or from the next message, until its COMMIT.
    /// </summary>
    private static void ApplyChanges(byte[]? firstChange)
    {
        if (firstChange is not null)
        {
            ApplyChange(firstChange, true);
        }

        long statusInterval = StatusInterval();
        bool reported = false;

        PgReplicationConnection conn = s_applyconn ?? throw new InvalidOperationException("The apply connection is not open.");
        while (s_commitEndLsn is null)
        {
            if (Worker.GotSigterm)
            {
                throw new OperationCanceledException("The worf worker is stopping.");
            }

            PgInterrupts.Check();

            PgReplicationReceiveStatus status = conn.Receive(TimeSpan.FromMilliseconds(1000), out ReadOnlySpan<byte> copybuf);
            while (status == PgReplicationReceiveStatus.Message && s_commitEndLsn is null)
            {
                HandleCopyData(copybuf.ToArray(), inTransaction: true);

                if (s_commitEndLsn is not null)
                {
                    break;
                }

                // The walsender drops a connection it hears nothing from for wal_sender_timeout, and the keepalive it
                // sends to ask for a reply waits behind everything already sent, which a large transaction can take
                // longer than that to apply. So report progress meanwhile.
                long now = Worker.GetCurrentTimestamp();
                if (statusInterval > 0 && now - s_lastSendTime >= statusInterval)
                {
                    _ = SendFeedback(conn, s_lastReceived, now, true);
                    reported = true;
                }

                // Cleanup the memory. pglogical does this after each read from the socket, which libpq empties before
                // reading again, so a busy stream can't keep the context growing; this reads one message at a time, so
                // it cleans up after each.
                NativeMethods.MemoryContextReset(NativeGlobals.MessageContext);

                status = conn.Receive(TimeSpan.Zero, out copybuf);
            }

            if (status == PgReplicationReceiveStatus.Ended)
            {
                throw new PgException(PgSqlStates.InternalError, "data stream ended");
            }

            if (s_commitEndLsn is null)
            {
                // confirm all writes at once
                SendFeedback(conn, s_lastReceived, Worker.GetCurrentTimestamp(), false);

                // Cleanup the memory.
                NativeMethods.MemoryContextReset(NativeGlobals.MessageContext);
            }
        }

        // Committing a transaction this large can take a while too, as when it dropped many tables, and nothing can be
        // reported meanwhile, so give the commit all of wal_sender_timeout.
        if (reported)
        {
            _ = SendFeedback(conn, s_lastReceived, Worker.GetCurrentTimestamp(), true);
        }
    }

    /// <summary>
    /// Figure out which write/flush positions to report to the walsender process.
    /// </summary>
    /// <returns>Whether there's no outstanding transactions that need to be flushed.</returns>
    private static bool GetFlushPosition(out ulong write, out ulong flush)
    {
        ulong localFlush = NativeMethods.GetFlushRecPtr(null);

        write = 0;
        flush = 0;

        while (s_lsnMapping.First is { } node)
        {
            write = node.Value.RemoteEnd;

            if (node.Value.LocalEnd <= localFlush)
            {
                flush = node.Value.RemoteEnd;
                s_lsnMapping.RemoveFirst();
            }
            else
            {
                // Don't want to uselessly iterate over the rest of the list which could potentially be long. Instead
                // get the last element and grab the write position from there.
                write = s_lsnMapping.Last!.Value.RemoteEnd;
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Gets how often to report progress, as a walreceiver does, in microseconds.
    /// </summary>
    private static long StatusInterval() => NativeGlobals.wal_receiver_status_interval * 1000000L;

    /// <summary>
    /// Send a Standby Status Update message to server.
    /// </summary>
    private static bool SendFeedback(PgReplicationConnection conn, ulong recvpos, long now, bool force)
    {
        // It's legal to not pass a recvpos
        if (recvpos < s_lastRecvpos)
        {
            recvpos = s_lastRecvpos;
        }

        if (GetFlushPosition(out ulong writepos, out ulong flushpos))
        {
            // No outstanding transactions to flush, we can report the latest received position. This is important
            // for synchronous replication.
            flushpos = writepos = recvpos;
        }

        if (writepos < s_lastWritepos)
        {
            writepos = s_lastWritepos;
        }

        if (flushpos < s_lastFlushpos)
        {
            flushpos = s_lastFlushpos;
        }

        // if we've already reported everything we're good, unless nothing was reported for wal_receiver_status_interval.
        // The walsender drops a connection it hears nothing from for wal_sender_timeout, and while one transaction's
        // decoding keeps it busy that long it can't ask for a reply, so report anyway, as a walreceiver does.
        if (!force && writepos == s_lastWritepos && flushpos == s_lastFlushpos)
        {
            long statusInterval = StatusInterval();
            if (statusInterval <= 0 || now - s_lastSendTime < statusInterval)
            {
                return true;
            }
        }

        Span<byte> replyMessage = stackalloc byte[1 + 8 + 8 + 8 + 8 + 1];
        replyMessage[0] = (byte)'r';
        BinaryPrimitives.WriteUInt64BigEndian(replyMessage[1..], recvpos);        // write
        BinaryPrimitives.WriteUInt64BigEndian(replyMessage[9..], flushpos);       // flush
        BinaryPrimitives.WriteUInt64BigEndian(replyMessage[17..], writepos);      // apply
        BinaryPrimitives.WriteInt64BigEndian(replyMessage[25..], now);            // sendTime
        replyMessage[33] = 0;                                                     // replyRequested

        PgLog.Debug2($"sending feedback (force {(force ? 1 : 0)}) to recv {Worf.FormatLsn(recvpos)}, write {Worf.FormatLsn(writepos)}, flush {Worf.FormatLsn(flushpos)}");

        conn.Send(replyMessage);
        s_lastSendTime = now;

        if (recvpos > s_lastRecvpos)
        {
            s_lastRecvpos = recvpos;
        }

        if (writepos > s_lastWritepos)
        {
            s_lastWritepos = writepos;
        }

        if (flushpos > s_lastFlushpos)
        {
            s_lastFlushpos = flushpos;
        }

        return true;
    }

    /// <summary>
    /// Load list of tables currently pending sync. Must be inside transaction.
    /// </summary>
    private static void RereadUnsyncedTables(uint subid) => s_syncingTables = Sync.GetUnsyncedTables(subid);

    private static void ProcessSyncingTables(ulong endLsn)
    {
        WorfApplyWorker myApplyWorker = MyApplyWorker;
        uint subid = myApplyWorker.SubId;
        uint myDatabaseId = NativeGlobals.MyDatabaseId;

        // First check if we need to update the cached information.
        if (myApplyWorker.SyncPending)
        {
            PgBackgroundWorker.RunTransaction(() =>
            {
                SetSyncPending(false);
                RereadUnsyncedTables(subid);
            });
        }

        // Process currently pending sync tables.
        if (s_syncingTables.Count > 0)
        {
            for (int index = 0; index < s_syncingTables.Count; index++)
            {
                WorfSyncStatus sync = s_syncingTables[index];

                PgBackgroundWorker.RunTransaction(() =>
                {
                    WorfSyncStatus? newsync = Sync.GetTableSyncStatus(subid, sync.Nspname, sync.Relname, true);

                    // TODO: what to do here? We don't really want to die, but this can mean many things, for now we
                    // just assume table is not relevant for us anymore and leave fixing to the user.
                    if (newsync is null)
                    {
                        sync.Status = Sync.SyncStatusReady;
                        sync.Statuslsn = 0;
                    }
                    else
                    {
                        sync.Kind = newsync.Kind;
                        sync.Status = newsync.Status;
                        sync.Statuslsn = newsync.Statuslsn;
                    }
                });

                if (sync.Status == Sync.SyncStatusSyncwait)
                {
                    bool caughtUp = false;
                    byte[] syncNspname = Worf.ToServer(sync.Nspname);
                    byte[] syncRelname = Worf.ToServer(sync.Relname);
                    using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
                    {
                        int worker = guard.Read((in _, workers) =>
                            Worker.SyncFind(workers, myDatabaseId, subid, syncNspname, syncRelname));

                        if (worker >= 0 && guard[worker].Proc != 0 && endLsn >= guard[worker].Worker.Apply.ReplayStopLsn)
                        {
                            WorfWorker w = guard[worker];
                            w.Worker.Apply.ReplayStopLsn = endLsn;
                            guard[worker] = w;
                            sync.Status = Sync.SyncStatusCatchup;

                            PgBackgroundWorker.RunTransaction(() =>
                                Sync.SetTableSyncStatus(subid, sync.Nspname, sync.Relname, sync.Status, sync.Statuslsn));

                            if (guard[worker].Proc != 0)
                            {
                                Worker.SetProcLatch(guard[worker].Proc);
                            }

                            caughtUp = true;
                        }
                    }

                    // The sync worker's catch-up can outlast wal_sender_timeout, so keep the walsender from dropping
                    // this connection, which nothing reads from meanwhile.
                    if (caughtUp &&
                        Sync.WaitForSyncStatusChange(subid, sync.Nspname, sync.Relname, Sync.SyncStatusSyncdone, out ulong statuslsn,
                            static () => SendFeedback(s_applyconn!, s_lastReceived, Worker.GetCurrentTimestamp(), true)))
                    {
                        sync.Statuslsn = statuslsn;
                        sync.Status = Sync.SyncStatusSyncdone;
                    }
                }

                if (sync.Status == Sync.SyncStatusSyncdone && endLsn >= sync.Statuslsn)
                {
                    sync.Status = Sync.SyncStatusReady;
                    sync.Statuslsn = endLsn;

                    PgBackgroundWorker.RunTransaction(() =>
                        Sync.SetTableSyncStatus(subid, sync.Nspname, sync.Relname, sync.Status, sync.Statuslsn));
                }

                // Ready? Remove it from local cache.
                if (sync.Status == Sync.SyncStatusReady)
                {
                    s_syncingTables.RemoveAt(index);
                    index--;
                }
            }
        }

        // If there are still pending tables for synchronization, launch the sync worker.
        foreach (WorfSyncStatus sync in s_syncingTables)
        {
            if (sync.Status is Sync.SyncStatusSyncdone or Sync.SyncStatusReady)
            {
                continue;
            }

            int nworkers = 0;
            using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
            {
                nworkers = guard.Read((in _, workers) =>
                {
                    int running = 0;
                    foreach (int worker in Worker.SyncFindAll(workers, myDatabaseId, subid))
                    {
                        running += Worker.WorkerRunning(workers, worker) ? 1 : 0;
                    }

                    return running;
                });
            }

            if (nworkers < 1)
            {
                StartSyncWorker(sync.Nspname, sync.Relname);
                break;
            }
        }
    }

    private static void StartSyncWorker(string nspname, string relname)
    {
        WorfWorker me = Worker.MyWorker;

        // Start the sync worker.
        WorfWorker worker = default;
        worker.WorkerType = WorfWorkerType.Sync;
        worker.Dboid = me.Dboid;
        worker.Worker.Apply.SubId = me.Worker.Apply.SubId;
        worker.Worker.Apply.SyncPending = false; // Makes no sense for sync worker.

        // Tell the worker to stop at current position.
        worker.Worker.Sync.Apply.ReplayStopLsn = Worf.ReploriginSessionOriginLsn;
        Worker.NameStrCpy(ref worker.Worker.Sync.Nspname, nspname);
        Worker.NameStrCpy(ref worker.Worker.Sync.Relname, relname);

        _ = Worker.WorkerRegister(worker);
    }

    private static void SetSyncPending(bool syncPending)
    {
        using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive();
        WorfWorker worker = guard[Worker.MyWorfWorker];
        worker.Worker.Apply.SyncPending = syncPending;
        guard[Worker.MyWorfWorker] = worker;
    }

    private static int JsonbType(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Null => 0,
        JsonValueKind.String => 1,
        JsonValueKind.Number => 2,
        JsonValueKind.True or JsonValueKind.False => 3,
        JsonValueKind.Array => 0x10,
        _ => 0x11,
    };

    /// <summary>
    /// Parses the leading integer of a string, as <c>strtoll</c> does.
    /// </summary>
    private static long ParseInt64Prefix(string value)
    {
        int end = 0;
        while (end < value.Length && char.IsWhiteSpace(value[end]))
        {
            end++;
        }

        int start = end;
        if (end < value.Length && (value[end] == '-' || value[end] == '+'))
        {
            end++;
        }

        while (end < value.Length && char.IsAsciiDigit(value[end]))
        {
            end++;
        }

        return long.TryParse(value.AsSpan(start, end - start), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long result)
            ? result
            : 0;
    }
}
