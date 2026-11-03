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
            Argument = (nuint)slot,
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
    /// the worker is ready and give it access to the worker's PGPROC.
    /// </summary>
    /// <param name="slot">The worker's slot.</param>
    /// <param name="type">The worker's type.</param>
    internal static void WorkerAttach(int slot, WorfWorkerType type)
    {
        uint dboid;
        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive())
        {
            _ = PgProcess.OnExit(static code => WorkerDetach(code != 0));

            MyWorfWorker = slot;
            WorfWorker worker = guard[slot];
            unsafe
            {
                worker.Proc = (nint)NativeGlobals.MyProc;
            }

            guard[slot] = worker;
            s_myWorfWorkerGeneration = worker.Generation;
            dboid = worker.Dboid;

            WorfLog.Debug2($"{WorkerTypeName(type)} worker [{Environment.ProcessId}] attaching to slot {slot} generation {s_myWorfWorkerGeneration}");
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
                unsafe
                {
                    fixed (byte* name = Worf.ToServerCString("search_path"))
                    fixed (byte* value = Worf.ToServerCString(Worf.PinnedSearchPath))
                    {
                        _ = NativeMethods.set_config_option((sbyte*)name, (sbyte*)value, GucContext.PGC_SUSET, GucSource.PGC_S_SESSION,
                            GucAction.GUC_ACTION_SET, true, 0, false);
                    }
                }
            }
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
    /// up the slots of relay origins they no longer need.
    /// </summary>
    internal static void WantOriginSlot()
    {
        using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive();
        WorfContext ctx = guard.Header;
        ctx.OriginSlotWantedAt = GetCurrentTimestamp();
        guard.Header = ctx;
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

            // A prepared transaction's workers are signalled as it is prepared, as nothing hooks COMMIT PREPARED.
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
    /// This is our own version of WaitForBackgroundWorkerStartup where we wait until worker actually attaches to
    /// our shmem.
    /// </summary>
    private static void WaitForWorkerStartup(int slot, ushort generation, PgBackgroundWorkerHandle handle)
    {
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
            // installed in this database. Don't wake the supervisor immediately - let it restart on its normal
            // timeout. This prevents rapid cycling for databases without worf.
            if (worker.WorkerType == WorfWorkerType.Manager && !worker.SkipSupervisorWakeup)
            {
                ctx.SubscriptionsChanged = true;
            }

            // The free slot of a manager that had nothing to manage keeps its database, so that the supervisor doesn't
            // start it again at each look.
            worker.IdleAt = worker.WorkerType == WorfWorkerType.Manager && worker.SkipSupervisorWakeup ? GetCurrentTimestamp() : 0;
            worker.Dboid = worker.IdleAt != 0 ? worker.Dboid : 0;
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
