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
    /// Register the worf worker proccess.
    /// </summary>
    /// <param name="worker">The worker to start.</param>
    /// <returns>The assigned slot number.</returns>
    internal static int WorkerRegister(WorfWorker worker)
    {
        int slot;
        ushort generation;
        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive())
        {
            slot = guard.Mutate((ref ctx, workers) => FindEmptyWorkerSlot(workers, worker.Dboid));
            if (slot == -1)
            {
                throw new PgException(PgSqlStates.InternalError,
                    "could not register worf worker: all background worker slots are already used");
            }

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
            name = $"worf sync {Worf.ShortenHash(NameStr(worker.Worker.Sync.Relname), NameDataLen - 37),27} " +
                $"{worker.Dboid}:{worker.Worker.Sync.Apply.SubId}";
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

            throw new PgException(PgSqlStates.ConfigurationLimitExceeded,
                "worker registration failed, you might want to increase max_worker_processes setting");
        }

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

            PgLog.Debug2($"{WorkerTypeName(type)} worker [{Environment.ProcessId}] attaching to slot {slot} generation {s_myWorfWorkerGeneration}");
        }

        // Make it easy to identify our processes.
        SetConfigOption("application_name", PgBackgroundWorker.Name);

        // Connect to database if needed.
        if (dboid != 0)
        {
            PgBackgroundWorker.Connect(dboid, 0);
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
            });
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
        for (int i = 0; i < workers.Length; i++)
        {
            if (workers[i].WorkerType == WorfWorkerType.None ||
                (workers[i].CrashedAt != 0 && (workers[i].Dboid == dboid || workers[i].Dboid == 0)))
            {
                return i;
            }
        }

        return -1;
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
                PgLog.Debug1("worf supervisor exiting on SIGTERM");
                throw new OperationCanceledException("The worf worker is stopping.");
            }

            PgBackgroundWorkerState state = handle.GetState();
            using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive())
            {
                WorfWorker worker = guard[slot];
                if (state.Status == PgBackgroundWorkerStatus.Started && worker.Proc != 0)
                {
                    PgLog.Debug2($"{WorkerTypeName(worker.WorkerType)} worker at slot {slot} started with pid {state.ProcessId} and attached to shmem");
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
                        PgLog.Debug2($"{WorkerTypeName(worker.WorkerType)} worker at slot {slot} exited prematurely");
                        worker.CrashedAt = GetCurrentTimestamp();
                        guard[slot] = worker;
                    }
                    else
                    {
                        // Worker exited normally or self-reported a crash and may have already been replaced. Either
                        // way, we don't care, we're only looking for crashes before shmem attach.
                        PgLog.Debug2($"{WorkerTypeName(worker.WorkerType)} worker at slot {slot} exited before we noticed it started");
                    }

                    break;
                }
            }

            // Poll for worker startup. On Windows, use a shorter interval since process creation is slower and we
            // want to detect attachment to shared memory as quickly as possible.
            if (!PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(OperatingSystem.IsWindows() ? 200 : 1000)))
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

        PgLog.Log($"{WorkerTypeName(type)} worker [{Environment.ProcessId}] at slot {MyWorfWorker} generation {s_myWorfWorkerGeneration} {(crash ? "exiting with error" : "detaching cleanly")}");

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

            worker.WorkerType = WorfWorkerType.None;
            worker.Dboid = 0;
            worker.SkipSupervisorWakeup = false;
        }

        // If we notified that subscriptions changed (manager exit), wake up the supervisor so it can restart the
        // manager immediately rather than waiting for the latch timeout.
        if (ctx.SubscriptionsChanged && ctx.Supervisor != 0)
        {
            supervisor = ctx.Supervisor;
        }

        // Likewise wake the manager when an apply or sync worker exits: a subscription change that stops a worker
        // wakes the manager when it commits, and the manager can look before the worker has gone.
        if (type is WorfWorkerType.Apply or WorfWorkerType.Sync)
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

            PgLog.Debug2($"killing worf {WorkerTypeName(worker.WorkerType)} worker [{pid}] at slot {slot}");
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
