using System.Globalization;
using System.Runtime.InteropServices;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// worf worker helper functions.
/// </summary>
internal static class Worker
{
    /// <summary>
    /// The size of a <c>NameData</c>.
    /// </summary>
    internal const int NameDataLen = 64;

    /// <summary>
    /// The shared context and its worker slots, behind one lightweight lock.
    /// </summary>
    internal static PgLwLock<WorfContext, WorfWorker> WorfCtx { get; } = new("worf_context");

    /// <summary>
    /// Gets the shared table of prepared transactions whose COMMIT PREPARED wakes managers.
    /// </summary>
    internal static PgLwLock<WorfPreparedTable, WorfPrepared> Prepared { get; } = new("worf_prepared");

    // The most prepared transactions the table holds at once; once it's full, the oldest is forgotten, and its COMMIT
    // PREPARED leaves the managers to their next look.
    private const int MaxPrepared = 64;

    private static readonly List<SignalWorkerItem> s_signalWorkers = [];

    private static bool s_xacthookSignalWorkers;

    // Whether the transaction's commit starts every database's manager, even one that had nothing to manage.
    private static bool s_forgetEveryIdleManager;

    // Whether a registration found every background worker slot taken since one last succeeded.
    private static bool s_outOfWorkerSlots;

    private static ushort s_myWorfWorkerGeneration;

    /// <summary>
    /// Gets or sets the slot of this process's worker, or -1.
    /// </summary>
    internal static int MyWorfWorker { get; set; } = -1;

    // How many calls to a provider with a deadline this worker is within.
    private static int s_providerCalls;

    /// <summary>
    /// Init shmem needed for workers.
    /// </summary>
    internal static void WorkerShmemInit()
    {
        int nworkers;
        unsafe
        {
            nworkers = NativeGlobals.max_worker_processes;
        }

        // Whether this is a first startup or crash recovery, we'll be re-initing the bgworkers.
        MyWorfWorker = -1;

        PgSharedMemory.Initialize(WorfCtx, nworkers,
            () => new WorfContext { Supervisor = 0, SubscriptionsChanged = false, TotalWorkers = nworkers },
            static _ => EmptyWorker());
        PgSharedMemory.Initialize(Prepared, MaxPrepared, static () => default, static _ => default);
    }

    /// <summary>
    /// Register the worf worker proccess. With every background worker slot taken, it warns, as PostgreSQL's logical
    /// replication launcher does, once until a worker registers again, and the caller tries again later.
    /// </summary>
    /// <param name="worker">The worker to start.</param>
    /// <returns>The assigned slot number, or -1 when every background worker slot is taken.</returns>
    internal static int WorkerRegister(WorfWorker worker)
    {
        int slot;
        ushort generation = 0;
        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive())
        {
            slot = guard.Mutate((ref ctx, workers) => FindEmptyWorkerSlot(workers, worker.Dboid));
            if (slot != -1)
            {
                WorfWorker workerShm = guard[slot];

                // Maintain a generation counter for worker registrations; see WaitForWorkerStartup(...). The counter
                // wraps around.
                generation = workerShm.Generation == ushort.MaxValue ? (ushort)0 : (ushort)(workerShm.Generation + 1);

                workerShm = worker;
                workerShm.Generation = generation;
                workerShm.CrashedAt = 0;
                workerShm.Proc = 0;
                workerShm.WorkerType = worker.WorkerType;
                workerShm.SkipSupervisorWakeup = false;
                workerShm.TerminateRequested = false;
                guard[slot] = workerShm;
            }
        }

        if (slot == -1)
        {
            OutOfWorkerSlots();
            return -1;
        }

        string name;
        string function;
        if (worker.WorkerType == WorfWorkerType.Manager)
        {
            function = "worf_manager_main";
            name = $"worf manager {worker.Dboid}";
        }
        else if (worker.WorkerType == WorfWorkerType.Sync)
        {
            function = "worf_sync_main";
            // A worker's name is ASCII, so a table name's other characters show as question marks, followed by a
            // hash of the name, which tells tables apart whose names differ only in those characters.
            string table = NameStr(worker.Worker.Sync.Relname);
            string relname = new([.. table.Select(c => char.IsAscii(c) ? c : '?')]);
            relname = relname == table ? Worf.ShortenHash(relname, NameDataLen - 37)
                : Worf.Truncate(relname, NameDataLen - 45) + Worf.HashAny(table).ToString("x8", CultureInfo.InvariantCulture)[..7];
            name = $"worf sync {relname,27} {worker.Dboid}:{worker.Worker.Sync.Apply.SubId}";
        }
        else if (worker.WorkerType == WorfWorkerType.Range)
        {
            function = "worf_range_main";
            name = $"worf range {worker.Dboid}";
        }
        else if (worker.WorkerType == WorfWorkerType.Exchange)
        {
            function = "worf_exchange_main";
            name = $"worf exchange {worker.Dboid}";
        }
        else if (worker.WorkerType == WorfWorkerType.Switch)
        {
            function = "worf_switch_main";
            name = $"worf switch {worker.Dboid}:{worker.Worker.Switch.Seqoid}";
        }
        else
        {
            function = "worf_apply_main";
            name = $"worf apply {worker.Dboid}:{worker.Worker.Apply.SubId}";
        }

        int myProcPid;
        unsafe
        {
            myProcPid = NativeGlobals.MyProcPid;
        }

        PgBackgroundWorkerOptions bgw = new(name, Worf.ExtensionName, function)
        {
            DatabaseAccess = true,
            StartTime = PgBackgroundWorkerStartTime.RecoveryFinished,
            RestartDelay = null,
            NotifyProcessId = myProcPid,
            Argument = Argument(slot, generation),
        };

        if (!PgBackgroundWorker.TryStart(bgw, out PgBackgroundWorkerHandle? bgwHandle))
        {
            using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive())
            {
                WorfWorker workerShm = guard[slot];
                workerShm.CrashedAt = GetCurrentTimestamp();
                guard[slot] = workerShm;
            }

            OutOfWorkerSlots();
            return -1;
        }

        s_outOfWorkerSlots = false;
        using (bgwHandle)
        {
            WaitForWorkerStartup(slot, generation, bgwHandle);
        }

        // Waiting for startup resets the latch, which may have been set for the caller, as when the new worker has
        // already crashed. Set it again so the caller's wait loop looks again instead of sleeping through it.
        unsafe
        {
            NativeMethods.SetLatch(NativeGlobals.MyLatch);
        }

        return slot;
    }

    /// <summary>
    /// Warns that every background worker slot is taken, once until a worker registers again.
    /// </summary>
    private static void OutOfWorkerSlots()
    {
        if (!s_outOfWorkerSlots)
        {
            s_outOfWorkerSlots = true;
            WorfLog.Warning(new PgDiagnostic("out of background worker slots")
            {
                SqlState = PgSqlStates.ConfigurationLimitExceeded,
                Hint = "You might need to increase \"max_worker_processes\".",
            });
        }
    }

    /// <summary>
    /// Attach the current master process to the shared context. Called during worker startup to inform the master
    /// the worker is ready and give it access to the worker's PGPROC. A worker that whoever registered it gave up on
    /// before it attached, as one that took too long to start, finds its slot marked crashed or registered again, and
    /// exits without touching it.
    /// </summary>
    /// <param name="mainArg">The worker's argument, which names its slot and the slot's generation.</param>
    /// <param name="type">The worker's type.</param>
    internal static void WorkerAttach(nuint mainArg, WorfWorkerType type)
    {
        int slot = (int)(uint)mainArg;
        ushort generation = (ushort)(mainArg >> 32);
        uint dboid = 0;
        bool givenUp;
        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive())
        {
            WorfWorker worker = guard[slot];
            givenUp = worker.Generation != generation || worker.WorkerType != type || worker.CrashedAt != 0 || worker.Proc != 0;
            if (!givenUp)
            {
                Attach(guard, slot, worker);
                dboid = worker.Dboid;
            }
        }

        if (givenUp)
        {
            WorfLog.Log($"{WorkerTypeName(type)} worker [{Environment.ProcessId}] at slot {slot} generation {generation} exiting, as it was given up on before it started");
            ProcExit(0);
        }

        // Make it easy to identify our processes.
        SetConfigOption("application_name", PgBackgroundWorker.Name);

        // Connect to database if needed. A worker runs as the bootstrap superuser. Apply and sync workers keep the search
        // path the database or its role sets, as pglogical does, for the triggers and other user code their changes run;
        // worf's own SQL in them runs with the system catalog alone ahead of temporary objects. The other workers run
        // only worf's own work, all of it with that path.
        if (dboid != 0)
        {
            PgBackgroundWorker.Connect(dboid, 0);

            // A worker has no client. In a database whose encoding converts to UTF-8, the encoding of the text that
            // crosses into and out of .NET, its client encoding is UTF-8, so that PostgreSQL converts that text with the
            // conversion it keeps for the client, which works between transactions too. The copies a sync worker reads
            // name their encoding, and apply reads the values it receives in the database encoding, as
            // Worf.InDatabaseEncoding has it.
            PgBackgroundWorker.RunTransaction(() =>
            {
                if (Worf.ConvertsEncoding)
                {
                    unsafe
                    {
                        fixed (byte* name = "client_encoding\0"u8)
                        fixed (byte* value = "UTF8\0"u8)
                        {
                            _ = NativeMethods.set_config_option((sbyte*)name, (sbyte*)value, GucContext.PGC_SUSET, GucSource.PGC_S_SESSION,
                                GucAction.GUC_ACTION_SET, true, 0, false);
                        }
                    }
                }

                Worf.PrepareEncodingConversions();
            });
            if (type is not (WorfWorkerType.Apply or WorfWorkerType.Sync))
            {
                SetSessionOption("search_path", Worf.PinnedSearchPath);
            }

            // A wait on a heavyweight lock that outlasts deadlock_timeout is logged with the lock and its holders, as
            // from PostgreSQL 18 by default, unless the server's configuration says otherwise: a worker has no
            // client to show it to, and apply waits on locks that sessions hold for as long as they hold them, as
            // PostgreSQL's own apply does. The manager, which every apply worker in the database waits on to start,
            // gives up on a lock after a while instead, and tries again on its next look.
            unsafe
            {
                fixed (byte* name = "log_lock_waits\0"u8)
                fixed (byte* value = "on\0"u8)
                {
                    NativeMethods.SetConfigOption((sbyte*)name, (sbyte*)value, GucContext.PGC_SUSET, GucSource.PGC_S_DYNAMIC_DEFAULT);
                }
            }

            if (type == WorfWorkerType.Manager)
            {
                SetSessionOption("lock_timeout", $"{Manager.LockTimeout}ms");
            }
        }
    }

    /// <summary>
    /// Sets a setting for the rest of the session, as <c>SET</c> does.
    /// </summary>
    private static unsafe void SetSessionOption(string name, string value)
    {
        fixed (byte* cname = Worf.ToServerCString(name))
        fixed (byte* cvalue = Worf.ToServerCString(value))
        {
            _ = NativeMethods.set_config_option((sbyte*)cname, (sbyte*)cvalue, GucContext.PGC_SUSET, GucSource.PGC_S_SESSION,
                GucAction.GUC_ACTION_SET, true, 0, false);
        }
    }

    /// <summary>
    /// Gets a worker's slot contents. The caller holds the lock.
    /// </summary>
    /// <param name="guard">The caller's lock.</param>
    /// <param name="slot">The slot.</param>
    /// <returns>The worker.</returns>
    internal static WorfWorker GetWorker(PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard, int slot) => guard[slot];

    /// <summary>
    /// Sets a setting for the rest of the process, as <c>SetConfigOption</c> with <c>PGC_BACKEND</c> and
    /// <c>PGC_S_OVERRIDE</c> does.
    /// </summary>
    /// <param name="name">The setting.</param>
    /// <param name="value">The value.</param>
    internal static void SetConfigOption(string name, string value) => SetConfigOption(name, value, GucContext.PGC_BACKEND);

    /// <summary>
    /// Sets a setting for the rest of the process with <c>PGC_S_OVERRIDE</c>, as <c>SetConfigOption</c> does.
    /// </summary>
    /// <param name="name">The setting.</param>
    /// <param name="value">The value.</param>
    /// <param name="context">The context to set it in.</param>
    internal static unsafe void SetConfigOption(string name, string value, GucContext context)
    {
        fixed (byte* cname = Worf.ToServerCString(name))
        fixed (byte* cvalue = Worf.ToServerCString(value))
        {
            NativeMethods.SetConfigOption((sbyte*)cname, (sbyte*)cvalue, context, GucSource.PGC_S_OVERRIDE);
        }
    }

    /// <summary>
    /// Waits on the process latch for it to be set or for a timeout, then resets it, as an ordinary backend does
    /// with <c>WaitLatch</c>, and exits when the postmaster has died.
    /// </summary>
    /// <param name="timeoutMs">The timeout in milliseconds.</param>
    internal static void WaitLatch(int timeoutMs)
    {
        const int WlLatchSet = 1;
        const int WlTimeout = 8;
        const int WlPostmasterDeath = 16;
        const uint PgWaitExtension = 0x07000000;

        int rc;
        unsafe
        {
            rc = NativeMethods.WaitLatch(NativeGlobals.MyLatch, WlLatchSet | WlTimeout | WlPostmasterDeath, timeoutMs, PgWaitExtension);
            NativeMethods.ResetLatch(NativeGlobals.MyLatch);
        }

        // emergency bailout if postmaster has died
        if ((rc & WlPostmasterDeath) != 0)
        {
            ProcExit(1);
        }
    }

    /// <summary>
    /// Exits the process with an exit code, as <c>proc_exit</c> does.
    /// </summary>
    /// <param name="code">The exit code.</param>
    internal static void ProcExit(int code)
    {
        unsafe
        {
            NativeMethods.proc_exit(code);
        }
    }

    /// <summary>
    /// Gets a copy of this process's worker slot, as <c>MyPGLogicalWorker</c> points to it.
    /// </summary>
    internal static WorfWorker MyWorker
    {
        get
        {
            using PgLwLockShareGuard<WorfContext, WorfWorker> guard = WorfCtx.Share();
            return guard[MyWorfWorker];
        }
    }

    /// <summary>
    /// Gets when a worker last found no replication origin slot free.
    /// </summary>
    internal static long OriginSlotWantedAt
    {
        get
        {
            using PgLwLockShareGuard<WorfContext, WorfWorker> guard = WorfCtx.Share();
            return guard.Header.OriginSlotWantedAt;
        }
    }

    /// <summary>
    /// Gets when a worker last gave up replication origin slots for PostgreSQL subscriptions' origins.
    /// </summary>
    internal static long OriginSlotsGivenUpAt
    {
        get
        {
            using PgLwLockShareGuard<WorfContext, WorfWorker> guard = WorfCtx.Share();
            return guard.Header.OriginSlotsGivenUpAt;
        }
    }

    /// <summary>
    /// Records that this worker gave up replication origin slots for PostgreSQL subscriptions' origins, which every
    /// worker leaves them for a while.
    /// </summary>
    internal static void GaveUpOriginSlots()
    {
        using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive();
        WorfContext ctx = guard.Header;
        ctx.OriginSlotsGivenUpAt = GetCurrentTimestamp();
        guard.Header = ctx;
    }

    /// <summary>
    /// Records that this worker found no replication origin slot free, so that the workers of other subscriptions give
    /// up the slots of relay origins they no longer need, and, for a subscription's or table sync's own origin, without
    /// which it can't apply anything, even those they used last.
    /// </summary>
    /// <param name="own">Whether the slot is for a subscription's or table sync's own origin, rather than a relay
    /// origin.</param>
    /// <returns>When the want was recorded.</returns>
    internal static long WantOriginSlot(bool own)
    {
        using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive();
        WorfContext ctx = guard.Header;
        long now = GetCurrentTimestamp();
        ctx.OriginSlotWantedAt = now;
        if (own)
        {
            ctx.OwnOriginSlotWantedAt = now;
        }

        guard.Header = ctx;
        return now;
    }

    /// <summary>
    /// Gets when a worker last found no replication origin slot free for its subscription's or table sync's own origin.
    /// </summary>
    internal static long OwnOriginSlotWantedAt
    {
        get
        {
            using PgLwLockShareGuard<WorfContext, WorfWorker> guard = WorfCtx.Share();
            return guard.Header.OwnOriginSlotWantedAt;
        }
    }

    /// <summary>
    /// Gets the current time as a <c>TimestampTz</c>.
    /// </summary>
    /// <returns>Microseconds since 2000-01-01.</returns>
    internal static long GetCurrentTimestamp()
    {
        unsafe
        {
            return NativeMethods.GetCurrentTimestamp();
        }
    }

    /// <summary>
    /// Find the manager worker for given database. The caller holds the lock.
    /// </summary>
    /// <param name="workers">The worker slots.</param>
    /// <param name="dboid">The database.</param>
    /// <returns>The manager's slot, or -1.</returns>
    internal static int ManagerFind(ReadOnlySpan<WorfWorker> workers, uint dboid)
    {
        for (int i = 0; i < workers.Length; i++)
        {
            if (workers[i].WorkerType == WorfWorkerType.Manager && dboid == workers[i].Dboid)
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Find the apply worker for given subscription. The caller holds the lock.
    /// </summary>
    /// <param name="workers">The worker slots.</param>
    /// <param name="dboid">The database.</param>
    /// <param name="subscriberid">The subscription.</param>
    /// <returns>The apply worker's slot, or -1.</returns>
    internal static int ApplyFind(ReadOnlySpan<WorfWorker> workers, uint dboid, uint subscriberid)
    {
        for (int i = 0; i < workers.Length; i++)
        {
            if (workers[i].WorkerType == WorfWorkerType.Apply && dboid == workers[i].Dboid &&
                subscriberid == workers[i].Worker.Apply.SubId)
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Find all apply workers for given database. The caller holds the lock.
    /// </summary>
    /// <param name="workers">The worker slots.</param>
    /// <param name="dboid">The database.</param>
    /// <returns>The apply workers' slots.</returns>
    internal static List<int> ApplyFindAll(ReadOnlySpan<WorfWorker> workers, uint dboid)
    {
        List<int> res = [];
        for (int i = 0; i < workers.Length; i++)
        {
            if (workers[i].WorkerType == WorfWorkerType.Apply && dboid == workers[i].Dboid)
            {
                res.Add(i);
            }
        }

        return res;
    }

    /// <summary>
    /// Find the sync worker for given subscription and table. The caller holds the lock.
    /// </summary>
    /// <param name="workers">The worker slots.</param>
    /// <param name="dboid">The database.</param>
    /// <param name="subscriberid">The subscription.</param>
    /// <param name="nspname">The table's schema, in the server encoding.</param>
    /// <param name="relname">The table's name, in the server encoding.</param>
    /// <returns>The sync worker's slot, or -1.</returns>
    /// <remarks>
    /// The names are compared as the server's bytes, because no backend function can run while the slots are
    /// borrowed.
    /// </remarks>
    internal static int SyncFind(ReadOnlySpan<WorfWorker> workers, uint dboid, uint subscriberid, byte[] nspname, byte[] relname)
    {
        for (int i = 0; i < workers.Length; i++)
        {
            WorfWorker w = workers[i];
            if (w.WorkerType == WorfWorkerType.Sync && dboid == w.Dboid && subscriberid == w.Worker.Apply.SubId &&
                NameBytes(w.Worker.Sync.Nspname).SequenceEqual(nspname) &&
                NameBytes(w.Worker.Sync.Relname).SequenceEqual(relname))
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Find the sync workers for given subscription. The caller holds the lock.
    /// </summary>
    /// <param name="workers">The worker slots.</param>
    /// <param name="dboid">The database.</param>
    /// <param name="subscriberid">The subscription.</param>
    /// <returns>The sync workers' slots.</returns>
    internal static List<int> SyncFindAll(ReadOnlySpan<WorfWorker> workers, uint dboid, uint subscriberid)
    {
        List<int> res = [];
        for (int i = 0; i < workers.Length; i++)
        {
            WorfWorker w = workers[i];
            if (w.WorkerType == WorfWorkerType.Sync && dboid == w.Dboid && subscriberid == w.Worker.Apply.SubId)
            {
                res.Add(i);
            }
        }

        return res;
    }

    /// <summary>
    /// Is the worker running?
    /// </summary>
    /// <param name="workers">The worker slots.</param>
    /// <param name="slot">The worker's slot, or -1.</param>
    /// <returns>Whether a process is attached to the slot.</returns>
    internal static bool WorkerRunning(ReadOnlySpan<WorfWorker> workers, int slot) => slot >= 0 && workers[slot].Proc != 0;

    /// <summary>
    /// Sets a process's latch.
    /// </summary>
    /// <param name="proc">The process's <c>PGPROC</c>.</param>
    internal static unsafe void SetProcLatch(nint proc) => NativeMethods.SetLatch(&((PGPROC*)proc)->procLatch);

    /// <summary>
    /// Wakes this database's manager worker, if it runs.
    /// </summary>
    internal static void WakeManager()
    {
        uint dboid;
        unsafe
        {
            dboid = NativeGlobals.MyDatabaseId;
        }

        using PgLwLockShareGuard<WorfContext, WorfWorker> guard = WorfCtx.Share();
        int manager = guard.Read((in _, workers) => ManagerFind(workers, dboid));
        if (manager >= 0 && guard[manager].Proc != 0)
        {
            SetProcLatch(guard[manager].Proc);
        }
    }

    /// <summary>
    /// Wakes this database's sequence range worker, if it runs.
    /// </summary>
    internal static void WakeRangeWorker()
    {
        uint dboid;
        unsafe
        {
            dboid = NativeGlobals.MyDatabaseId;
        }

        using PgLwLockShareGuard<WorfContext, WorfWorker> guard = WorfCtx.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].WorkerType == WorfWorkerType.Range && guard[i].Dboid == dboid && guard[i].Proc != 0)
            {
                SetProcLatch(guard[i].Proc);
            }
        }
    }

    /// <summary>
    /// Has the transaction, once it commits, wake the supervisor to start the manager of this database, or of every
    /// database, even if it had nothing to manage, as once CREATE EXTENSION worf or ALTER DATABASE commits.
    /// </summary>
    /// <param name="everyDatabase">Whether every database's manager starts, rather than this database's.</param>
    internal static void ForgetIdleManagersAtCommit(bool everyDatabase)
    {
        s_forgetEveryIdleManager |= everyDatabase;
        SubscriptionChanged(0, false);
    }

    /// <summary>
    /// Enqueue signal for supervisor/manager at COMMIT.
    /// </summary>
    /// <param name="subid">The subscription whose apply worker to signal, or zero.</param>
    /// <param name="kill">Whether to stop the apply worker.</param>
    internal static void SubscriptionChanged(uint subid, bool kill)
    {
        if (!s_xacthookSignalWorkers)
        {
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, SignalWorkerXactCallback);
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, static () =>
            {
                s_signalWorkers.Clear();
                s_xacthookSignalWorkers = false;
                s_forgetEveryIdleManager = false;
            });

            // A prepared transaction's workers are signalled as it is prepared, and again once COMMIT PREPARED has
            // committed it, which the utility hook sees.
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Prepare, SignalWorkerXactCallback);
        }

        if (subid != 0)
        {
            s_signalWorkers.Add(new SignalWorkerItem(subid, kill));
        }

        s_xacthookSignalWorkers = true;
    }

    /// <summary>
    /// Gets a worker type's name.
    /// </summary>
    /// <param name="type">The type.</param>
    /// <returns>The name.</returns>
    internal static string WorkerTypeName(WorfWorkerType type) => type switch
    {
        WorfWorkerType.None => "none",
        WorfWorkerType.Manager => "manager",
        WorfWorkerType.Apply => "apply",
        WorfWorkerType.Sync => "sync",
        WorfWorkerType.Range => "range",
        WorfWorkerType.Exchange => "exchange",
        WorfWorkerType.Switch => "switch",
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };

    /// <summary>
    /// Reads a name's text, up to its terminator.
    /// </summary>
    /// <param name="name">The name.</param>
    /// <returns>The text.</returns>
    internal static string NameStr(in nameData name) => Worf.FromServer(NameBytes(name));

    /// <summary>
    /// Gets a name's bytes in the server encoding, up to its terminating zero.
    /// </summary>
    /// <param name="name">The name.</param>
    /// <returns>The bytes.</returns>
    internal static ReadOnlySpan<byte> NameBytes(in nameData name)
    {
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes((ReadOnlySpan<sbyte>)name.data);
        int length = bytes.IndexOf((byte)0);
        return length < 0 ? bytes : bytes[..length];
    }

    /// <summary>
    /// Copies text into a name, truncating it as <c>namestrcpy</c> does.
    /// </summary>
    /// <param name="name">The name.</param>
    /// <param name="str">The text.</param>
    internal static void NameStrCpy(ref nameData name, string str)
    {
        Span<byte> bytes = MemoryMarshal.AsBytes((Span<sbyte>)name.data);
        bytes.Clear();
        byte[] server = Worf.ToServer(str);
        server.AsSpan(0, Math.Min(server.Length, bytes.Length - 1)).CopyTo(bytes);
    }

    /// <summary>
    /// Returns a zeroed worker slot, as the memset of the shared context's workers produces.
    /// </summary>
    private static WorfWorker EmptyWorker()
    {
        WorfWorker worker = new()
        {
            WorkerType = WorfWorkerType.None,
            Generation = 0,
            Proc = 0,
            CrashedAt = 0,
            Dboid = 0,
            SkipSupervisorWakeup = false,
            TerminateRequested = false,
        };
        worker.Worker.Sync.Apply.SubId = 0;
        worker.Worker.Sync.Apply.SyncPending = false;
        worker.Worker.Sync.Apply.ReplayStopLsn = 0;
        worker.Worker.Sync.Nspname = default;
        worker.Worker.Sync.Relname = default;
        worker.Worker.Apply = worker.Worker.Sync.Apply;
        return worker;
    }

    /// <summary>
    /// Find unused worker slot. The caller is responsible for locking.
    /// </summary>
    private static int FindEmptyWorkerSlot(ReadOnlySpan<WorfWorker> workers, uint dboid)
    {
        // A free slot that remembers a database whose manager had nothing to manage is taken last.
        int idle = -1;
        for (int i = 0; i < workers.Length; i++)
        {
            if (workers[i].WorkerType == WorfWorkerType.None && workers[i].IdleAt != 0 && workers[i].Dboid != dboid)
            {
                idle = idle < 0 ? i : idle;
                continue;
            }

            if (workers[i].WorkerType == WorfWorkerType.None ||
                (workers[i].CrashedAt != 0 && (workers[i].Dboid == dboid || workers[i].Dboid == 0)))
            {
                return i;
            }
        }

        return idle;
    }

    /// <summary>
    /// Attaches this process to its slot, under the workers' lock, and has it detach as the process exits.
    /// </summary>
    private static void Attach(PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard, int slot, WorfWorker worker)
    {
        _ = PgProcess.OnExit(static code => WorkerDetach(code != 0));

        MyWorfWorker = slot;
        unsafe
        {
            worker.Proc = (nint)NativeGlobals.MyProc;
        }

        guard[slot] = worker;
        s_myWorfWorkerGeneration = worker.Generation;

        WorfLog.Debug2($"{WorkerTypeName(worker.WorkerType)} worker [{Environment.ProcessId}] attaching to slot {slot} generation {s_myWorfWorkerGeneration}");
    }

    /// <summary>
    /// The argument a worker starts with: its slot, and in the upper half, the slot's generation, which it checks as it
    /// attaches.
    /// </summary>
    private static nuint Argument(int slot, ushort generation) => (uint)slot | ((nuint)generation << 32);

    /// <summary>
    /// This is our own version of WaitForBackgroundWorkerStartup where we wait until worker actually attaches to
    /// our shmem. As PostgreSQL's logical replication launcher does, it gives up on a worker that hasn't attached
    /// within <c>wal_receiver_timeout</c>, as one whose process never started or hangs before it attaches: it warns,
    /// marks the slot crashed, so that the worker starts again later and nothing waits on it meanwhile, and stops the
    /// worker, which exits as it attaches if it gets that far.
    /// </summary>
    private static void WaitForWorkerStartup(int slot, ushort generation, PgBackgroundWorkerHandle handle)
    {
        int timeout = ProviderTimeout;
        long deadline = GetCurrentTimestamp() + (timeout * 1000L);
        (string Kind, uint SubId, string? Table, string Detail)? gaveUp = null;
        while (true)
        {
            PgInterrupts.Check();

            if (GotSigterm)
            {
                WorfLog.Debug1("worf supervisor exiting on SIGTERM");
                throw new OperationCanceledException("The worf worker is stopping.");
            }

            PgBackgroundWorkerState state = handle.GetState();
            using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive())
            {
                WorfWorker worker = guard[slot];
                if (state.Status == PgBackgroundWorkerStatus.Started && worker.Proc != 0)
                {
                    WorfLog.Debug2($"{WorkerTypeName(worker.WorkerType)} worker at slot {slot} started with pid {state.ProcessId} and attached to shmem");
                    break;
                }

                if (state.Status == PgBackgroundWorkerStatus.Stopped)
                {
                    // The worker may have:
                    // - failed to launch after registration
                    // - launched then crashed/exited before attaching
                    // - launched, attached, done its work, detached cleanly and exited before we got rescheduled
                    // - launched, attached, crashed and self-reported its crash, then exited before we got
                    //   rescheduled
                    //
                    // If it detached cleanly it will've set its worker type to None, which it can't have been at
                    // entry, so we know it must've started, attached and cleared it. However, someone else might've
                    // grabbed the slot and re-used it and not exited yet, so if the worker type is not None we can't
                    // tell if it's our worker that's crashed or another worker that might still be running. We use a
                    // generation counter incremented on registration to tell the difference.
                    if (worker.WorkerType != WorfWorkerType.None && worker.Generation == generation && worker.CrashedAt == 0)
                    {
                        // The worker we launched (same generation) crashed before attaching to shmem so it didn't
                        // set crashed_at. Mark it crashed so the slot can be re-used.
                        WorfLog.Debug2($"{WorkerTypeName(worker.WorkerType)} worker at slot {slot} exited prematurely");
                        worker.CrashedAt = GetCurrentTimestamp();
                        guard[slot] = worker;
                    }
                    else
                    {
                        // Worker exited normally or self-reported a crash and may have already been replaced. Either
                        // way, we don't care, we're only looking for crashes before shmem attach.
                        WorfLog.Debug2($"{WorkerTypeName(worker.WorkerType)} worker at slot {slot} exited before we noticed it started");
                    }

                    break;
                }

                if (GetCurrentTimestamp() >= deadline)
                {
                    if (worker.WorkerType != WorfWorkerType.None && worker.Generation == generation && worker.CrashedAt == 0)
                    {
                        worker.CrashedAt = GetCurrentTimestamp();
                        guard[slot] = worker;
                    }

                    handle.Terminate();
                    gaveUp = (WorkerTypeName(worker.WorkerType), worker.WorkerType is WorfWorkerType.Apply or WorfWorkerType.Sync ? worker.Worker.Apply.SubId : 0,
                        worker.WorkerType == WorfWorkerType.Sync ? Worf.QuoteQualifiedIdentifier(NameStr(worker.Worker.Sync.Nspname), NameStr(worker.Worker.Sync.Relname)) : null,
                        state.Status == PgBackgroundWorkerStatus.Started
                            ? $"Its process {state.ProcessId} hadn't attached to worf's shared memory after {timeout} ms."
                            : $"The postmaster hadn't started its process after {timeout} ms.");
                    break;
                }
            }

            // Poll for worker startup. A worker that has started attaches to shared memory soon after, which the
            // postmaster signals nothing for, so the poll is short then. On Windows, use a shorter interval since
            // process creation is slower and we want to detect attachment to shared memory as quickly as possible.
            if (!PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(state.Status == PgBackgroundWorkerStatus.Started ? 10
                : OperatingSystem.IsWindows() ? 200 : 1000)))
            {
                throw new OperationCanceledException("The worf worker is stopping.");
            }
        }

        // The warning names the subscription the worker was for, read once the slots are let go.
        if (gaveUp is (string kind, uint subId, var table, string detail))
        {
            string? subscription = subId == 0 ? null
                : PgBackgroundWorker.RunTransaction(() => Worf.ReadScalar<string?>("SELECT sub_name::text FROM worf.subscription WHERE sub_id = $1",
                    SpiParameter.Create(subId)));
            string what = subscription is null ? "" : table is null ? $" for subscription \"{subscription}\"" : $" for table {table} of subscription \"{subscription}\"";
            WorfLog.Warning(new PgDiagnostic($"{kind} worker{what} at slot {slot} took too long to start; canceled") { Detail = detail });
        }
    }

    /// <summary>
    /// The deadline the manager leaves on a call to a provider it cancels until it has sent the cancel, which the call, if
    /// it ended on its own meanwhile, waits for.
    /// </summary>
    internal const long CancellingCall = -2;

    /// <summary>
    /// Gets how long, in milliseconds, a call to another server, or a worker's start, may take: wal_receiver_timeout, as
    /// PostgreSQL's apply worker and launcher take, or its default, a minute, when it's zero.
    /// </summary>
    internal static int ProviderTimeout
    {
        get
        {
            unsafe
            {
                return NativeGlobals.wal_receiver_timeout > 0 ? NativeGlobals.wal_receiver_timeout : 60_000;
            }
        }
    }

    /// <summary>
    /// Gets <see cref="ProviderTimeout"/> as messages name it.
    /// </summary>
    internal static string ProviderTimeoutShown
    {
        get
        {
            unsafe
            {
                return NativeGlobals.wal_receiver_timeout > 0
                    ? $"wal_receiver_timeout ({NativeGlobals.wal_receiver_timeout} ms)"
                    : "a minute (wal_receiver_timeout is 0)";
            }
        }
    }

    /// <summary>
    /// Makes a call to a provider from an apply or table sync worker, as a connection or a query, giving up once
    /// <see cref="ProviderTimeout"/> has passed without an answer, as a provider that accepts a connection and then
    /// answers nothing would hold the worker for good: the deadline goes in the worker's slot, and the database's manager
    /// cancels the call once it has passed, since cancelling a query cancels a wait on the provider. A call made within
    /// another has the other's deadline. Elsewhere the call runs as it is. Within a transaction, the cancellation ends
    /// the transaction, as it isn't an error a subtransaction catches, so the error's context names what the call waited
    /// for.
    /// </summary>
    /// <typeparam name="T">The call's result.</typeparam>
    /// <param name="what">What the call waits for, as the error names it.</param>
    /// <param name="call">The call.</param>
    /// <returns>Its result.</returns>
    internal static T WithProviderDeadline<T>(Func<string> what, Func<T> call)
    {
        if (s_providerCalls > 0 || MyWorfWorker < 0 || MyWorker.WorkerType is not (WorfWorkerType.Apply or WorfWorkerType.Sync))
        {
            return call();
        }

        long deadline = GetCurrentTimestamp() + (ProviderTimeout * 1000L);
        bool inTransaction;
        unsafe
        {
            inTransaction = NativeMethods.IsTransactionState();
        }

        _ = SetCallDeadline(deadline);
        s_providerCalls++;
        bool canceled = false;
        try
        {
            if (!inTransaction)
            {
                return call();
            }

            using PgErrorContext waiting = PgErrorContext.Push(Worf.Shown($"waiting up to {ProviderTimeoutShown} for {what()}"));
            return call();
        }
        catch (Exception error) when (error is PgQueryCanceledException or PgException { SqlState: "57014" })
        {
            canceled = true;
            if (GetCurrentTimestamp() >= deadline)
            {
                throw new PgException(PgSqlStates.ConnectionFailure, $"timed out waiting for {what()}",
                    detail: $"It hadn't answered within {ProviderTimeoutShown}.");
            }

            throw;
        }
        finally
        {
            s_providerCalls--;

            // A call the manager cancelled that ended first takes the cancel that comes after it, so that it lands in
            // no later call, nor in the stream, a copy or an applied transaction.
            if (SetCallDeadline(0) is 0 or CancellingCall && !canceled)
            {
                _ = AwaitCancel(static () => CancelComing(), ProviderTimeout * 1000L);
            }
        }
    }

    /// <summary>
    /// Waits for the cancel of a call that its watcher cancelled as it ended on its own, and takes it: until the watcher
    /// has sent it, as it shows once <paramref name="sending"/> is false, and it has come, or, should the watcher fail
    /// first, for a limit. In a transaction, the cancel is taken in a subtransaction that it rolls back, as an exception
    /// block takes an error.
    /// </summary>
    /// <param name="sending">Whether the watcher is still to send the cancel.</param>
    /// <param name="limit">How long, in microseconds, to wait for the watcher.</param>
    /// <returns>Whether the cancel came; if not, it may still come.</returns>
    internal static bool AwaitCancel(Func<bool> sending, long limit)
    {
        bool inTransaction;
        unsafe
        {
            inTransaction = NativeMethods.IsTransactionState();
        }

        long started = GetCurrentTimestamp();
        long sentAt = 0;
        while (true)
        {
            try
            {
                if (inTransaction)
                {
                    if (PgInterrupts.IsPending)
                    {
                        PgTransaction.RunInSubtransaction(PgInterrupts.Check);
                    }
                }
                else
                {
                    PgInterrupts.Check();
                }
            }
            catch (PgQueryCanceledException)
            {
                return true;
            }

            long now = GetCurrentTimestamp();
            if (sentAt == 0 && !sending())
            {
                sentAt = now;
            }

            if ((sentAt != 0 && now - sentAt >= 1_000_000) || now - started >= limit)
            {
                return false;
            }

            WaitLatch(10);
        }
    }

    /// <summary>
    /// Checks whether the manager is still to send the cancel of this worker's call that it marked: not once it has
    /// marked it sent, nor once it has exited, which leaves the mark, so that a call whose cancel never comes waits a
    /// second for it at most.
    /// </summary>
    private static bool CancelComing()
    {
        using PgLwLockShareGuard<WorfContext, WorfWorker> guard = WorfCtx.Share();
        WorfWorker me = guard[MyWorfWorker];
        if (me.Worker.Apply.CallDeadline != CancellingCall)
        {
            return false;
        }

        int manager = guard.Read((in _, workers) => ManagerFind(workers, me.Dboid));
        return manager >= 0 && guard[manager].Proc != 0;
    }

    /// <summary>
    /// Makes a call to a provider, as the other overload does.
    /// </summary>
    /// <param name="what">What the call waits for, as the error names it.</param>
    /// <param name="call">The call.</param>
    internal static void WithProviderDeadline(Func<string> what, Action call) => _ = WithProviderDeadline(what, () =>
    {
        call();
        return true;
    });

    /// <summary>
    /// Publishes the deadline of the call to the provider about to start, waking the manager to watch it, or clears it
    /// as the call ends, unless the manager is cancelling it: the manager marks the cancel sent itself.
    /// </summary>
    /// <returns>The deadline it replaced: zero or <see cref="CancellingCall"/>, as a call ends, if the manager cancelled
    /// the call.</returns>
    private static long SetCallDeadline(long deadline)
    {
        long replaced;
        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive())
        {
            WorfWorker worker = guard[MyWorfWorker];
            replaced = worker.Worker.Apply.CallDeadline;
            if (replaced != CancellingCall || deadline != 0)
            {
                worker.Worker.Apply.CallDeadline = deadline;
                guard[MyWorfWorker] = worker;
            }
        }

        if (deadline != 0)
        {
            WakeManager();
        }

        return replaced;
    }

    /// <summary>
    /// Detach the current master process from the shared context. Called during master worker exit.
    /// </summary>
    private static void WorkerDetach(bool crash)
    {
        // Nothing to detach.
        if (MyWorfWorker < 0)
        {
            return;
        }

        nint supervisor = 0;
        nint manager = 0;
        WorfWorkerType type;
        using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive();
        WorfContext ctx = guard.Header;
        WorfWorker worker = guard[MyWorfWorker];
        worker.Proc = 0;
        type = worker.WorkerType;
        uint dboid = worker.Dboid;

        WorfLog.Log($"{WorkerTypeName(type)} worker [{Environment.ProcessId}] at slot {MyWorfWorker} generation {s_myWorfWorkerGeneration} {(crash ? "exiting with error" : "detaching cleanly")}");

        // If we crashed we need to report it.
        //
        // The crash logic only works because all of the workers are attached to shmem and the serious crashes
        // that we can't catch here cause postmaster to restart whole server killing all our workers and cleaning
        // shmem so we start from clean state in that scenario.
        //
        // It's vital NOT to clear or change the generation field here; see WaitForWorkerStartup(...).
        if (crash)
        {
            worker.CrashedAt = GetCurrentTimestamp();

            // Manager crash, make sure supervisor notices.
            if (worker.WorkerType == WorfWorkerType.Manager)
            {
                ctx.SubscriptionsChanged = true;
            }

            // An apply worker that stopped to apply its failed transaction in a new way is restarted at once.
            if (worker.WorkerType == WorfWorkerType.Apply && worker.Worker.Apply.ExceptionRestartsAtOnce)
            {
                int managerSlot = guard.Read((in _, workers) => ManagerFind(workers, dboid));
                if (managerSlot >= 0 && guard[managerSlot].Proc != 0)
                {
                    SetProcLatch(guard[managerSlot].Proc);
                }
            }
        }
        else
        {
            // Worker has finished work, clean up its state from shmem. If this was a manager, notify the
            // supervisor so it can restart the manager if needed (e.g., if the database still has
            // subscriptions).
            //
            // However, if SkipSupervisorWakeup is set, the manager is exiting because the extension isn't
            // installed in this database, or the database is an idle template, and doesn't wake the supervisor, so
            // that a database without worf doesn't start its manager over and over, nor a template keep a connection
            // that copying it would wait for; a change, as CREATE EXTENSION worf, wakes the supervisor.
            //
            // A manager whose look began before a transaction that may have given it something to manage, as CREATE
            // EXTENSION worf, committed may not have seen it, and so isn't idle: the supervisor starts it again at once.
            bool idle = worker.WorkerType == WorfWorkerType.Manager && worker.SkipSupervisorWakeup && worker.IdleAt > ctx.IdleForgottenAt;
            if (worker.WorkerType == WorfWorkerType.Manager && !idle)
            {
                ctx.SubscriptionsChanged = true;
            }

            // The free slot of a manager that had nothing to manage keeps its database, and when it looked, so that the
            // supervisor doesn't start it again at each look.
            worker.IdleAt = idle ? worker.IdleAt : 0;
            worker.Dboid = idle ? worker.Dboid : 0;
            worker.WorkerType = WorfWorkerType.None;
            worker.SkipSupervisorWakeup = false;
        }

        // If we notified that subscriptions changed (manager exit), wake up the supervisor so it can restart the
        // manager immediately rather than waiting for the latch timeout.
        if (ctx.SubscriptionsChanged && ctx.Supervisor != 0)
        {
            supervisor = ctx.Supervisor;
        }

        // Likewise wake the manager when an apply, sync, range or exchange worker exits: a subscription change that stops a
        // worker wakes the manager when it commits, and the manager can look before the worker has gone.
        if (type is WorfWorkerType.Apply or WorfWorkerType.Sync or WorfWorkerType.Range or WorfWorkerType.Exchange)
        {
            int managerSlot = guard.Read((in _, workers) => ManagerFind(workers, dboid));
            if (managerSlot >= 0)
            {
                manager = guard[managerSlot].Proc;
            }
        }

        guard[MyWorfWorker] = worker;
        guard.Header = ctx;
        MyWorfWorker = -1;

        if (supervisor != 0)
        {
            SetProcLatch(supervisor);
        }

        if (manager != 0)
        {
            SetProcLatch(manager);
        }
    }

    /// <summary>
    /// Wakes a database's apply workers, as once a session that kept another node's fence ends.
    /// </summary>
    /// <param name="dboid">The database.</param>
    internal static void WakeApplyWorkers(uint dboid)
    {
        List<nint> procs = [];
        using (PgLwLockShareGuard<WorfContext, WorfWorker> guard = WorfCtx.Share())
        {
            for (int i = 0; i < guard.Length; i++)
            {
                if (guard[i].WorkerType == WorfWorkerType.Apply && guard[i].Dboid == dboid && guard[i].Proc != 0)
                {
                    procs.Add(guard[i].Proc);
                }
            }
        }

        foreach (nint proc in procs)
        {
            SetProcLatch(proc);
        }
    }

    /// <summary>
    /// Forgets that a database's manager had nothing to manage, or every database's for zero, so that the supervisor
    /// starts it at its next look. The caller holds the lock exclusively.
    /// </summary>
    /// <param name="guard">The caller's lock.</param>
    /// <param name="dboid">The database, or zero.</param>
    internal static void ForgetIdleManager(PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard, uint dboid)
    {
        WorfContext ctx = guard.Header;
        ctx.IdleForgottenAt = GetCurrentTimestamp();
        guard.Header = ctx;
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].WorkerType == WorfWorkerType.None && guard[i].IdleAt != 0 && (dboid == 0 || guard[i].Dboid == dboid))
            {
                WorfWorker worker = guard[i];
                worker.IdleAt = 0;
                worker.Dboid = 0;
                guard[i] = worker;
            }
        }
    }

    /// <summary>
    /// Gets whether a database's manager had nothing to manage at its last start, since a time.
    /// </summary>
    /// <param name="workers">The worker slots.</param>
    /// <param name="dboid">The database.</param>
    /// <param name="since">The time.</param>
    /// <returns>Whether it had.</returns>
    internal static bool ManagerIdle(ReadOnlySpan<WorfWorker> workers, uint dboid, long since)
    {
        foreach (WorfWorker worker in workers)
        {
            if (worker.WorkerType == WorfWorkerType.None && worker.Dboid == dboid && worker.IdleAt > since)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Wakes the supervisor, as once a database is dropped, so that it frees the worker slots that database's workers
    /// left.
    /// </summary>
    internal static void WakeSupervisor()
    {
        nint supervisor;
        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive())
        {
            WorfContext ctx = guard.Header;
            ctx.SubscriptionsChanged = true;
            guard.Header = ctx;
            supervisor = ctx.Supervisor;
        }

        if (supervisor != 0)
        {
            SetProcLatch(supervisor);
        }
    }

    /// <summary>
    /// Notes, as PREPARE TRANSACTION prepares a transaction that created worf here, changed its nodes or subscriptions,
    /// or changed a database, as its workers' signals at commit say, that its COMMIT PREPARED wakes the managers, as
    /// <see cref="PreparedWakes"/> tells, by the hash of its name, so that no other COMMIT PREPARED reads anything. The table is in
    /// shared memory, so after a restart the managers learn of such a commit at their next look.
    /// </summary>
    /// <param name="hash">The hash of the transaction's name.</param>
    internal static void NotePrepared(uint hash)
    {
        if (!s_xacthookSignalWorkers)
        {
            return;
        }

        uint dboid;
        unsafe
        {
            dboid = NativeGlobals.MyDatabaseId;
        }

        long now = GetCurrentTimestamp();
        using PgLwLockExclusiveGuard<WorfPreparedTable, WorfPrepared> guard = Prepared.Exclusive();
        int slot = 0;
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].Dboid == 0 || (guard[i].Dboid == dboid && guard[i].GidHash == hash))
            {
                slot = i;
                break;
            }

            if (guard[i].PreparedAt < guard[slot].PreparedAt)
            {
                slot = i;
            }
        }

        guard[slot] = new WorfPrepared { Dboid = dboid, GidHash = hash, EveryDatabase = s_forgetEveryIdleManager, PreparedAt = now };
    }

    /// <summary>
    /// Tells whether COMMIT PREPARED of a transaction wakes managers, as <see cref="NotePrepared"/> noted, and whose.
    /// </summary>
    /// <param name="hash">The hash of the transaction's name.</param>
    /// <returns>Whether every database's managers wake, or only this one's, or null for none.</returns>
    internal static bool? PreparedWakes(uint hash)
    {
        uint dboid;
        unsafe
        {
            dboid = NativeGlobals.MyDatabaseId;
        }

        using PgLwLockShareGuard<WorfPreparedTable, WorfPrepared> guard = Prepared.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].Dboid == dboid && guard[i].GidHash == hash)
            {
                return guard[i].EveryDatabase;
            }
        }

        return null;
    }

    /// <summary>
    /// Forgets a prepared transaction once COMMIT PREPARED or ROLLBACK PREPARED has ended it.
    /// </summary>
    /// <param name="hash">The hash of the transaction's name.</param>
    internal static void ForgetPrepared(uint hash)
    {
        uint dboid;
        unsafe
        {
            dboid = NativeGlobals.MyDatabaseId;
        }

        using PgLwLockExclusiveGuard<WorfPreparedTable, WorfPrepared> guard = Prepared.Exclusive();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].Dboid == dboid && guard[i].GidHash == hash)
            {
                guard[i] = default;
            }
        }
    }

    /// <summary>
    /// Signals the database's manager and the supervisor once COMMIT PREPARED has committed a prepared transaction that
    /// created or changed worf here, or changed a database: they heard of it as it was prepared, before anyone could see
    /// what it did, and a manager that found nothing to manage then starts again, this database's or, after a change to
    /// a database, every one's.
    /// </summary>
    /// <param name="everyDatabase">Whether the transaction changed a database, rather than only worf here.</param>
    internal static void SignalPreparedCommitted(bool everyDatabase)
    {
        uint myDatabaseId;
        unsafe
        {
            myDatabaseId = NativeGlobals.MyDatabaseId;
        }

        List<nint> latches = [];
        using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive();
        WorfContext ctx = guard.Header;
        ctx.SubscriptionsChanged = true;
        guard.Header = ctx;
        ForgetIdleManager(guard, everyDatabase ? 0 : myDatabaseId);
        int manager = guard.Read((in _, workers) => ManagerFind(workers, myDatabaseId));
        if (manager >= 0 && guard[manager].Proc != 0)
        {
            latches.Add(guard[manager].Proc);
        }

        if (ctx.Supervisor != 0)
        {
            latches.Add(ctx.Supervisor);
        }

        foreach (nint proc in latches)
        {
            SetProcLatch(proc);
        }
    }

    private static void SignalWorkerXactCallback()
    {
        if (!s_xacthookSignalWorkers)
        {
            return;
        }

        uint myDatabaseId;
        unsafe
        {
            myDatabaseId = NativeGlobals.MyDatabaseId;
        }

        List<nint> latches = [];
        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive())
        {
            foreach (SignalWorkerItem item in s_signalWorkers)
            {
                int w = guard.Read((in _, workers) => ApplyFind(workers, myDatabaseId, item.SubId));
                if (item.Kill)
                {
                    WorkerKill(guard, w);
                }
                else if (w >= 0 && guard[w].Proc != 0)
                {
                    WorfWorker worker = guard[w];
                    worker.Worker.Apply.SyncPending = true;
                    guard[w] = worker;
                    latches.Add(worker.Proc);
                }
            }

            WorfContext ctx = guard.Header;
            ctx.SubscriptionsChanged = true;
            guard.Header = ctx;

            // The database, or after ALTER DATABASE any database, may have something to manage now.
            ForgetIdleManager(guard, s_forgetEveryIdleManager ? 0 : myDatabaseId);
            s_forgetEveryIdleManager = false;

            // Signal the manager worker, if there's one
            int manager = guard.Read((in _, workers) => ManagerFind(workers, myDatabaseId));
            if (manager >= 0 && guard[manager].Proc != 0)
            {
                latches.Add(guard[manager].Proc);
            }

            // and signal the supervisor, for good measure
            if (ctx.Supervisor != 0)
            {
                latches.Add(ctx.Supervisor);
            }

            foreach (nint proc in latches)
            {
                SetProcLatch(proc);
            }
        }

        s_signalWorkers.Clear();
        s_xacthookSignalWorkers = false;
    }

    /// <summary>
    /// Asks a running worker to stop. The caller holds the lock exclusively.
    /// </summary>
    /// <param name="guard">The caller's lock.</param>
    /// <param name="slot">The worker's slot, or -1.</param>
    internal static void WorkerKill(PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard, int slot)
    {
        if (slot < 0)
        {
            return;
        }

        WorfWorker worker = guard[slot];
        if (worker.Proc != 0)
        {
            int pid;
            unsafe
            {
                pid = ((PGPROC*)worker.Proc)->pid;
            }

            WorfLog.Debug2($"killing worf {WorkerTypeName(worker.WorkerType)} worker [{pid}] at slot {slot}");
            worker.TerminateRequested = true;
            guard[slot] = worker;
            SetProcLatch(worker.Proc);
        }
    }

    /// <summary>
    /// Reloads the configuration when the worker got SIGHUP.
    /// </summary>
    internal static void ProcessConfigReload()
    {
        if (PgBackgroundWorker.ConsumeSignals(PgBackgroundWorkerSignals.Reload) != PgBackgroundWorkerSignals.None)
        {
            PgBackgroundWorker.ReloadConfiguration();
        }
    }

    /// <summary>
    /// Gets whether this worker has been asked to stop, by another process or by PostgreSQL.
    /// </summary>
    internal static bool GotSigterm
    {
        get
        {
            if (!PgBackgroundWorker.CanContinue)
            {
                return true;
            }

            int slot = MyWorfWorker;
            if (slot < 0)
            {
                return false;
            }

            using PgLwLockShareGuard<WorfContext, WorfWorker> guard = WorfCtx.Share();
            return guard[slot].TerminateRequested;
        }
    }
}
