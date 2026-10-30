using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The worker that makes a database's exchanges with its neighbors, apart from its range worker, so that a neighbor that
/// never answers holds up none of the range worker's switches and Snowflake clock reservations. libpq keeps no time
/// limit of its own on a connection a backend waits for, so the range worker starts this worker and cancels any call of
/// it that outlasts <c>worf.sequence_range_timeout</c>. In a database that manages no sequence, it exchanges once, to
/// ask the providers whether they manage any, and exits.
/// </summary>
internal static class SequenceRangeExchangeWorker
{
    // How long the range worker waits before it starts an exchange worker again that exited with an error.
    private const long RestartDelay = 5_000_000;

    // When this range worker last started an exchange worker, or zero before it has.
    private static long s_startedAt;

    /// <summary>
    /// Runs the exchange worker.
    /// </summary>
    /// <param name="mainArg">The worker's slot.</param>
    [PgBackgroundWorker(EntryPoint = "worf_exchange_main")]
    internal static void ExchangeMain(nuint mainArg)
    {
        Worker.WorkerAttach((int)mainArg, WorfWorkerType.Exchange);
        PgLog.Debug1("starting sequence range exchange worker");
        while (!Worker.GotSigterm)
        {
            try
            {
                PgInterrupts.Check();
                Worker.ProcessConfigReload();

                // Backends reload their blocks once the change is committed.
                if (SequenceRangeExchange.ExchangeAll())
                {
                    SequenceRanges.Changed();
                }

                // A database that manages nothing has asked its providers, and the manager looks again later.
                if (!PgBackgroundWorker.RunTransaction(Managed))
                {
                    return;
                }

                _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(SequenceRanges.CheckInterval));
            }
            catch (PgQueryCanceledException)
            {
                // The range worker cancelled a call just as it ended, and the exchanges go on.
            }
        }
    }

    /// <summary>
    /// Keeps the database's exchange worker running, from its range worker, and cancels a call of the exchange worker
    /// that has run out of time.
    /// </summary>
    /// <param name="managed">Whether the database manages sequences, which keeps the exchange worker running. Otherwise
    /// it is started once.</param>
    /// <returns>Whether the exchange worker runs, or is to be started again.</returns>
    internal static bool Watch(bool managed)
    {
        uint dboid = Worker.MyWorker.Dboid;
        long now = Worker.GetCurrentTimestamp();
        bool running = false;
        int overdue = 0;
        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
        {
            for (int i = 0; i < guard.Length; i++)
            {
                WorfWorker worker = guard[i];
                if (worker.WorkerType != WorfWorkerType.Exchange || worker.Dboid != dboid || worker.CrashedAt != 0)
                {
                    continue;
                }

                running = true;
                if (worker.Proc != 0 && worker.Worker.Exchange.Deadline != 0 && now >= worker.Worker.Exchange.Deadline)
                {
                    unsafe
                    {
                        overdue = ((PGPROC*)worker.Proc)->pid;
                    }

                    worker.Worker.Exchange.Deadline = 0;
                    guard[i] = worker;
                }
            }
        }

        // The call fails in the exchange worker, which tries the neighbor again later.
        if (overdue != 0)
        {
            _ = PgBackgroundWorker.RunTransaction(() => Worf.ReadScalar<bool>("SELECT pg_catalog.pg_cancel_backend($1)", SpiParameter.Create(overdue)));
        }

        if (running)
        {
            return true;
        }

        if (s_startedAt != 0 && (!managed || now - s_startedAt < RestartDelay))
        {
            return managed;
        }

        s_startedAt = now;
        try
        {
            _ = Worker.WorkerRegister(new WorfWorker { WorkerType = WorfWorkerType.Exchange, Dboid = dboid });
        }
        catch (PgException error)
        {
            PgLog.Warning($"could not start the sequence range exchange worker: {error.Message}");
        }

        return true;
    }

    /// <summary>
    /// Wakes the database's exchange worker, as when a request waits to be sent.
    /// </summary>
    internal static void Wake()
    {
        uint dboid = Worker.MyWorker.Dboid;
        using PgLwLockShareGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].WorkerType == WorfWorkerType.Exchange && guard[i].Dboid == dboid && guard[i].Proc != 0)
            {
                Worker.SetProcLatch(guard[i].Proc);
            }
        }
    }

    /// <summary>
    /// Makes a call to a neighbor, from the exchange worker, which the range worker cancels once it has taken longer than
    /// <c>worf.sequence_range_timeout</c>.
    /// </summary>
    /// <typeparam name="T">The call's result.</typeparam>
    /// <param name="call">The call.</param>
    /// <returns>Its result.</returns>
    internal static T Bounded<T>(Func<T> call)
    {
        SetDeadline(Worker.GetCurrentTimestamp() + (SequenceRanges.ExchangeTimeout * 1000L));
        try
        {
            return call();
        }
        finally
        {
            SetDeadline(0);
        }
    }

    private static void SetDeadline(long deadline)
    {
        if (Worker.MyWorfWorker < 0)
        {
            return;
        }

        using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive();
        WorfWorker worker = guard[Worker.MyWorfWorker];
        worker.Worker.Exchange.Deadline = deadline;
        guard[Worker.MyWorfWorker] = worker;
    }

    private static bool Managed()
        => Worf.ReadScalar<bool>("SELECT to_regclass('worf.sequence_range') IS NOT NULL") &&
            Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range)");
}
