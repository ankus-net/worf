using System.Runtime.InteropServices;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The kinds of worf worker.
/// </summary>
internal enum WorfWorkerType
{
    /// <summary>
    /// Unused slot.
    /// </summary>
    None,

    /// <summary>
    /// Manager.
    /// </summary>
    Manager,

    /// <summary>
    /// Apply.
    /// </summary>
    Apply,

    /// <summary>
    /// Special type of Apply that synchronizes one table.
    /// </summary>
    Sync,
}

/// <summary>
/// An apply worker's shared state.
/// </summary>
internal struct WorfApplyWorker
{
    /// <summary>
    /// Subscription id for apply worker.
    /// </summary>
    public uint SubId;

    /// <summary>
    /// Is there new synchronization info pending?.
    /// </summary>
    public bool SyncPending;

    /// <summary>
    /// Replay should stop here if defined.
    /// </summary>
    public ulong ReplayStopLsn;
}

/// <summary>
/// A sync worker's shared state.
/// </summary>
internal struct WorfSyncWorker
{
    /// <summary>
    /// Apply worker info, must be first.
    /// </summary>
    public WorfApplyWorker Apply;

    /// <summary>
    /// Name of the schema of table to copy if any.
    /// </summary>
    public nameData Nspname;

    /// <summary>
    /// Name of the table to copy if any.
    /// </summary>
    public nameData Relname;
}

/// <summary>
/// Type-specific worker info.
/// </summary>
[StructLayout(LayoutKind.Explicit)]
internal struct WorfWorkerInfo
{
    /// <summary>
    /// The apply worker's info.
    /// </summary>
    [FieldOffset(0)]
    public WorfApplyWorker Apply;

    /// <summary>
    /// The sync worker's info.
    /// </summary>
    [FieldOffset(0)]
    public WorfSyncWorker Sync;
}

/// <summary>
/// A worker slot in shared memory.
/// </summary>
internal struct WorfWorker
{
    /// <summary>
    /// The worker's type.
    /// </summary>
    public WorfWorkerType WorkerType;

    /// <summary>
    /// Generation counter incremented at each registration.
    /// </summary>
    public ushort Generation;

    /// <summary>
    /// Pointer to proc array. Zero if not running.
    /// </summary>
    public nint Proc;

    /// <summary>
    /// Time at which worker crashed (normally 0).
    /// </summary>
    public long CrashedAt;

    /// <summary>
    /// Database id to connect to.
    /// </summary>
    public uint Dboid;

    /// <summary>
    /// Set by manager before exiting due to missing extension. Prevents supervisor from being woken immediately,
    /// avoiding rapid restart cycles for databases without worf.
    /// </summary>
    public bool SkipSupervisorWakeup;

    /// <summary>
    /// Type-specific worker info.
    /// </summary>
    public WorfWorkerInfo Worker;
}

/// <summary>
/// The shared state that the worker slots follow.
/// </summary>
internal struct WorfContext
{
    /// <summary>
    /// Supervisor process.
    /// </summary>
    public nint Supervisor;

    /// <summary>
    /// Signal that subscription info have changed.
    /// </summary>
    public bool SubscriptionsChanged;

    /// <summary>
    /// Background workers.
    /// </summary>
    public int TotalWorkers;
}

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
            slot = guard.Mutate((ref WorfContext ctx, Span<WorfWorker> workers) => FindEmptyWorkerSlot(workers, worker.Dboid));
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
    internal static unsafe void SetConfigOption(string name, string value)
    {
        fixed (byte* cname = Worf.ToServerCString(name))
        fixed (byte* cvalue = Worf.ToServerCString(value))
        {
            NativeMethods.SetConfigOption((sbyte*)cname, (sbyte*)cvalue, GucContext.PGC_BACKEND, GucSource.PGC_S_OVERRIDE);
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
    /// <param name="nspname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    /// <returns>The sync worker's slot, or -1.</returns>
    internal static int SyncFind(ReadOnlySpan<WorfWorker> workers, uint dboid, uint subscriberid, string nspname, string relname)
    {
        for (int i = 0; i < workers.Length; i++)
        {
            WorfWorker w = workers[i];
            if (w.WorkerType == WorfWorkerType.Sync && dboid == w.Dboid && subscriberid == w.Worker.Apply.SubId &&
                NameStr(w.Worker.Sync.Nspname) == nspname && NameStr(w.Worker.Sync.Relname) == relname)
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
    internal static void SubscriptionChanged(uint subid)
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
            s_signalWorkers.Add(new SignalWorkerItem(subid));
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
    internal static string NameStr(in nameData name)
    {
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes((ReadOnlySpan<sbyte>)name.data);
        int length = bytes.IndexOf((byte)0);
        return Worf.FromServer(length < 0 ? bytes : bytes[..length]);
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

            if (!PgBackgroundWorker.CanContinue)
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
        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = WorfCtx.Exclusive())
        {
            WorfContext ctx = guard.Header;
            WorfWorker worker = guard[MyWorfWorker];
            worker.Proc = 0;

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

            guard[MyWorfWorker] = worker;
            guard.Header = ctx;
            MyWorfWorker = -1;

            if (supervisor != 0)
            {
                SetProcLatch(supervisor);
            }
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
            guard.Mutate((ref WorfContext ctx, Span<WorfWorker> workers) =>
            {
                foreach (SignalWorkerItem item in s_signalWorkers)
                {
                    int w = ApplyFind(workers, myDatabaseId, item.SubId);
                    if (WorkerRunning(workers, w))
                    {
                        workers[w].Worker.Apply.SyncPending = true;
                        latches.Add(workers[w].Proc);
                    }
                }

                ctx.SubscriptionsChanged = true;

                // Signal the manager worker, if there's one
                int manager = ManagerFind(workers, myDatabaseId);
                if (WorkerRunning(workers, manager))
                {
                    latches.Add(workers[manager].Proc);
                }

                // and signal the supervisor, for good measure
                if (ctx.Supervisor != 0)
                {
                    latches.Add(ctx.Supervisor);
                }

                return true;
            });

            foreach (nint proc in latches)
            {
                SetProcLatch(proc);
            }
        }

        s_signalWorkers.Clear();
        s_xacthookSignalWorkers = false;
    }

    /// <summary>
    /// A worker to signal when the transaction commits.
    /// </summary>
    /// <param name="SubId">The subscription.</param>
    private readonly record struct SignalWorkerItem(uint SubId);
}
