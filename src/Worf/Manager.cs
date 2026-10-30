using Ankus.Postgres;

namespace Worf;

/// <summary>
/// worf worker for managing apply workers in a database.
/// </summary>
internal static class Manager
{
    private const long InitialSleep = 10000;
    private const long MaxSleep = 180000;

    // When the manager next runs the range worker to ask this database's providers of the sequences they manage, and
    // the exchange counter it last saw, which a backend that learned of a sequence raises.
    private static long s_discoveryAt;
    private static long s_discoveryRequested;

    /// <summary>
    /// Gets the minimum time the manager sleeps between checking for subscription changes. On Windows, process
    /// startup is slower and we need more responsive checking to avoid test timeouts.
    /// </summary>
    private static long MinSleep => OperatingSystem.IsWindows() ? 1000 : 5000;

    /// <summary>
    /// Entry point for manager worker.
    /// </summary>
    /// <param name="mainArg">The worker's slot.</param>
    [PgBackgroundWorker(EntryPoint = "worf_manager_main")]
    internal static void ManagerMain(nuint mainArg)
    {
        int slot = (int)mainArg;
        uint extoid = 0;
        long sleepTimer = InitialSleep;

        // Setup shmem.
        Worker.WorkerAttach(slot, WorfWorkerType.Manager);

        // Check if the extension is installed. We retry a few times with a short delay to handle the race condition
        // where the extension is being dropped and recreated (e.g., between init_fail and init regression tests).
        // Without this retry, we might exit just before the extension is created, causing the supervisor to not
        // restart us until the next latch timeout.
        for (int retry = 0; retry < 5; retry++)
        {
            extoid = PgBackgroundWorker.RunTransaction(static () =>
            {
                unsafe
                {
                    fixed (byte* name = Worf.ToServerCString(Worf.ExtensionName))
                    {
                        return NativeMethods.get_extension_oid((sbyte*)name, true);
                    }
                }
            });

            if (extoid != 0)
            {
                break;
            }

            // Extension not found, wait and retry
            if (retry < 4)
            {
                if (!PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(200)) && !PgBackgroundWorker.CanContinue)
                {
                    return;
                }

                PgInterrupts.Check();
            }
        }

        // If the extension is still not installed after retries, exit.
        if (extoid == 0)
        {
            // Mark that we're exiting due to missing extension so the detach handler doesn't wake the supervisor.
            // This prevents rapid restart cycles for databases that don't have worf installed.
            using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive();
            WorfWorker me = guard[Worker.MyWorfWorker];
            me.SkipSupervisorWakeup = true;
            guard[Worker.MyWorfWorker] = me;

            return;
        }

        PgBackgroundWorker.RunTransaction(static () =>
            PgLog.Log($"starting worf database manager for database {Worf.ReadScalar<string>("SELECT pg_catalog.current_database()::text")}"));

        // Brief delay before checking/upgrading extension version. This avoids a race condition where an explicit
        // ALTER EXTENSION UPDATE runs concurrently with our auto-upgrade check on fast systems.
        unsafe
        {
            NativeMethods.pg_usleep(100000);  // 100ms
        }

        // Use separate transaction to avoid lock escalation.
        PgBackgroundWorker.RunTransaction(Worf.ManageExtension);

        // Main wait loop.
        while (!Worker.GotSigterm)
        {
            Worker.ProcessConfigReload();

            // Launch the apply workers, and the range worker for managed sequences.
            bool processedAll = ManageApplyWorkers();
            processedAll &= ManageRangeWorker();

            // Handle sequences and update our sleep timer as necessary.
            sleepTimer = Sequences.SynchronizeSequences()
                ? Math.Min(sleepTimer * 2, MaxSleep)
                : Math.Max(sleepTimer / 2, MinSleep);

            // A database that subscribes but manages no sequence runs the range worker again when the next exchange is
            // due.
            long sleep = processedAll ? sleepTimer : MinSleep;
            long now = Worker.GetCurrentTimestamp();
            if (s_discoveryAt > now)
            {
                sleep = Math.Clamp((s_discoveryAt - now) / 1000, 1, sleep);
            }

            if (!PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(sleep)) &&
                !PgBackgroundWorker.CanContinue)
            {
                break;
            }

            PgInterrupts.Check();
        }
    }

    /// <summary>
    /// Starts the database's sequence range worker and its exchange worker when it has managed sequences and they aren't
    /// running, or once a crashed one's restart delay has passed. Until it manages one, a database with subscriptions runs
    /// them once an exchange interval, or at once when a backend learned of a sequence, to ask the providers of the
    /// sequences they manage. The exchange worker connects to them, so a provider that can't be reached holds up neither
    /// the manager nor the range worker, which cancels a call that takes too long.
    /// </summary>
    /// <returns>Whether nothing is left to retry.</returns>
    private static bool ManageRangeWorker()
    {
        uint dboid = Worker.MyWorker.Dboid;
        long now = Worker.GetCurrentTimestamp();
        (bool rangeRunning, bool rangeCrashed) = RangeWorkerState(WorfWorkerType.Range, dboid, now);
        (bool exchangeRunning, bool exchangeCrashed) = RangeWorkerState(WorfWorkerType.Exchange, dboid, now);
        if (rangeRunning && exchangeRunning)
        {
            return true;
        }

        (bool managed, bool linked) = PgBackgroundWorker.RunTransaction(static () =>
        {
            if (!Worf.ReadScalar<bool>("SELECT to_regclass('worf.sequence_range') IS NOT NULL") ||
                WorfNode.GetLocalNode(false, true) is not WorfLocalNode node)
            {
                return (false, false);
            }

            return (Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range)"),
                WorfNode.GetNodeSubscriptions(node.Node.Id, false).Exists(sub => sub.Enabled));
        });

        long requested = SequenceRanges.ExchangeRequested.Value;
        if (!managed && !linked)
        {
            // A subscription created later asks at once.
            s_discoveryAt = 0;
            return true;
        }

        bool started = true;
        if (managed || now >= s_discoveryAt || requested != s_discoveryRequested)
        {
            s_discoveryRequested = requested;
            s_discoveryAt = now + (SequenceRanges.ExchangeInterval * 1000L);

            // The exchange worker starts first, so that the range worker finds it to watch.
            started = StartRangeWorker(WorfWorkerType.Exchange, dboid, exchangeRunning || exchangeCrashed) &&
                StartRangeWorker(WorfWorkerType.Range, dboid, rangeRunning || rangeCrashed);
        }

        return started && !rangeCrashed && !exchangeCrashed;
    }

    /// <summary>
    /// Finds whether one of a database's range workers runs, or crashed too lately to start again.
    /// </summary>
    private static (bool Running, bool Crashed) RangeWorkerState(WorfWorkerType type, uint dboid, long now)
    {
        using PgLwLockShareGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].WorkerType == type && guard[i].Dboid == dboid)
            {
                return (guard[i].CrashedAt == 0, guard[i].CrashedAt > 0 && guard[i].CrashedAt + (MinSleep * 1000) > now);
            }
        }

        return (false, false);
    }

    /// <summary>
    /// Starts one of a database's range workers. With every worker slot taken, it waits for the next look rather than
    /// stop the manager.
    /// </summary>
    /// <returns>Whether it started, or wasn't to start.</returns>
    private static bool StartRangeWorker(WorfWorkerType type, uint dboid, bool skip)
    {
        if (skip)
        {
            return true;
        }

        try
        {
            _ = Worker.WorkerRegister(new WorfWorker { WorkerType = type, Dboid = dboid });
            return true;
        }
        catch (PgException error)
        {
            PgLog.Warning($"could not start the sequence {Worker.WorkerTypeName(type)} worker: {error.Message}");
            return false;
        }
    }

    /// <summary>
    /// Manage the apply workers - start new ones, kill old ones.
    /// </summary>
    private static bool ManageApplyWorkers()
    {
        bool ret = true;
        uint dboid = Worker.MyWorker.Dboid;

        // Get list of existing workers.
        List<int> workers;
        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
        {
            workers = guard.Read((in _, slots) => Worker.ApplyFindAll(slots, dboid));
        }

        bool found = PgBackgroundWorker.RunTransaction(() =>
        {
            // Get local node. If not found, return false and let the main loop wait and retry. This handles the race
            // condition where the extension is created but create_node() hasn't been called yet.
            WorfLocalNode? node = WorfNode.GetLocalNode(true, true);
            if (node is null)
            {
                return false;
            }

            // Get list of subscribers.
            List<WorfSubscription> subscriptions = WorfNode.GetNodeSubscriptions(node.Node.Id, false);
            List<(WorfSubscription Sub, WorfApplyWorker Crashed)> subsToStart = [];

            // Check for active workers for each subscription.
            foreach (WorfSubscription sub in subscriptions)
            {
                // Skip if subscriber not enabled. This must be called before the following search loop because we
                // want to kill any workers for disabled subscribers.
                if (!sub.Enabled)
                {
                    continue;
                }

                // Check if the subscriber already has registered worker.
                int apply = -1;
                WorfWorker applyWorker = default;
                using (PgLwLockShareGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Share())
                {
                    foreach (int w in workers)
                    {
                        if (guard[w].Worker.Apply.SubId == sub.Id)
                        {
                            apply = w;
                            applyWorker = guard[w];
                            break;
                        }
                    }
                }

                if (apply >= 0)
                {
                    _ = workers.Remove(apply);
                }

                // Skip if the worker was alrady registered.
                if (apply >= 0 && applyWorker.Proc != 0)
                {
                    continue;
                }

                // Check if this is crashed worker and if we want to restart it now.
                if (apply >= 0)
                {
                    if (applyWorker.CrashedAt != 0)
                    {
                        long restartTime = applyWorker.CrashedAt + (MinSleep * 1000);

                        // One that stopped to apply its failed transaction in a new way restarts at once.
                        if (restartTime > Worker.GetCurrentTimestamp() && !applyWorker.Worker.Apply.ExceptionRestartsAtOnce)
                        {
                            ret = false;
                            continue;
                        }
                    }
                    else
                    {
                        ret = false;
                        continue;
                    }
                }

                subsToStart.Add((sub, applyWorker.Worker.Apply));
            }

            foreach ((WorfSubscription sub, WorfApplyWorker crashed) in subsToStart)
            {
                WorfWorker apply = default;
                apply.WorkerType = WorfWorkerType.Apply;
                apply.Dboid = dboid;
                apply.Worker.Apply.SubId = sub.Id;
                apply.Worker.Apply.SyncPending = true;
                apply.Worker.Apply.ReplayStopLsn = 0;

                // A crashed worker's failed transaction is applied again under worf.exception_behaviour.
                apply.Worker.Apply.ExceptionLsn = crashed.ExceptionLsn;
                apply.Worker.Apply.ExceptionLevel = crashed.ExceptionLevel;

                _ = Worker.WorkerRegister(apply);
            }

            return true;
        });

        if (!found)
        {
            return false;
        }

        // Kill any remaining running workers that should not be running.
        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
        {
            foreach (int worker in workers)
            {
                Worker.WorkerKill(guard, worker);

                // Cleanup old info about crashed apply workers.
                WorfWorker w = guard[worker];
                if (w.CrashedAt != 0)
                {
                    PgLog.Debug2($"cleaning worf worker slot {worker}");
                    w.WorkerType = WorfWorkerType.None;
                    w.CrashedAt = 0;
                    guard[worker] = w;
                }
            }
        }

        return ret;
    }
}
