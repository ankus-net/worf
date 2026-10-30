using Ankus.Postgres;

namespace Worf;

/// <summary>
/// worf worker for managing apply workers in a database.
/// </summary>
internal static class Manager
{
    private const long InitialSleep = 10000;
    private const long MaxSleep = 180000;

    // When the manager next asks this database's providers of the sequences they manage, or long.MaxValue.
    private static long s_discoveryAt = long.MaxValue;

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

        // If the extension is still not installed after retries, exit. A template that isn't a node has nothing to
        // manage, and a manager would keep it busy, while PostgreSQL copies a database only from a template nobody is
        // connected to.
        if (extoid == 0 || PgBackgroundWorker.RunTransaction(static () =>
            Worf.ReadScalar<bool>("SELECT datistemplate FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()") &&
            WorfNode.GetLocalNode(false, true) is null))
        {
            // Mark that we're exiting with nothing to manage so the detach handler doesn't wake the supervisor.
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

            // A database that subscribes but manages no sequence asks its providers again when the next exchange is due.
            long sleep = processedAll ? sleepTimer : MinSleep;
            if (s_discoveryAt != long.MaxValue)
            {
                sleep = Math.Clamp((s_discoveryAt - Worker.GetCurrentTimestamp()) / 1000, 1, sleep);
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
    /// Starts the database's sequence range worker when it has managed sequences and the worker isn't running, or once
    /// a crashed one's restart delay has passed. Until it manages one, a database with subscriptions asks their
    /// providers, at the exchange interval, of the sequences they manage.
    /// </summary>
    /// <returns>Whether nothing is left to retry; an exchange to retry sets when the manager wakes.</returns>
    private static bool ManageRangeWorker()
    {
        s_discoveryAt = long.MaxValue;
        uint dboid = Worker.MyWorker.Dboid;
        long crashedAt = -1;
        using (PgLwLockShareGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Share())
        {
            for (int i = 0; i < guard.Length && crashedAt < 0; i++)
            {
                if (guard[i].WorkerType == WorfWorkerType.Range && guard[i].Dboid == dboid)
                {
                    crashedAt = guard[i].CrashedAt;
                }
            }
        }

        if (crashedAt == 0)
        {
            return true;
        }

        if (crashedAt > 0 && crashedAt + (MinSleep * 1000) > Worker.GetCurrentTimestamp())
        {
            return false;
        }

        bool managed = PgBackgroundWorker.RunTransaction(static () =>
        {
            if (!Worf.ReadScalar<bool>("SELECT to_regclass('worf.sequence_range') IS NOT NULL") ||
                WorfNode.GetLocalNode(false, true) is not WorfLocalNode node)
            {
                return false;
            }

            if (!Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range)"))
            {
                _ = SequenceRangeExchange.ExchangeAll(node);
                s_discoveryAt = SequenceRangeExchange.NextDueAt;
            }

            return Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range)");
        });
        if (managed)
        {
            _ = Worker.WorkerRegister(new WorfWorker { WorkerType = WorfWorkerType.Range, Dboid = dboid });
        }

        // The exchange's connection waits on the latch and resets it, which may have been set for the manager, as when
        // an apply worker exited to apply its transaction again. Set it again so the manager looks again instead of
        // sleeping through it.
        if (SequenceRangeExchange.Contacted)
        {
            unsafe
            {
                NativeMethods.SetLatch(NativeGlobals.MyLatch);
            }
        }

        if (managed)
        {
            s_discoveryAt = long.MaxValue;
        }

        return true;
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
