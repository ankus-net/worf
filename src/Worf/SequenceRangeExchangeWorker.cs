using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The worker that makes a database's exchanges with its neighbors, apart from its range worker, so that a neighbor that
/// never answers holds up none of the range worker's switches and Snowflake clock reservations. It waits on no
/// neighbor, and its watcher, the range worker, tells it to stop as the watcher stops. The manager starts both. In a
/// database that manages no sequence, the manager starts it alone, once an exchange interval, and watches it; it
/// exchanges once, to ask the providers whether they manage any, and exits.
/// </summary>
internal static class SequenceRangeExchangeWorker
{
    // The deadline a watcher that stops leaves, which tells the exchange worker to make no more calls and stop too, since
    // none would cancel a call that never ends.
    private const long Stopped = -1;

    // The deadline the watcher leaves on a call it cancels until it has sent the cancel, which the call, if it ended on
    // its own meanwhile, waits for.
    private const long Cancelling = -2;

    // Whether this exchange worker's watcher stopped.
    private static bool s_unwatched;


    /// <summary>
    /// Gets whether this exchange worker's watcher stopped, so that it makes no more calls.
    /// </summary>
    internal static bool Unwatched => s_unwatched;

    /// <summary>
    /// Runs the exchange worker.
    /// </summary>
    /// <param name="mainArg">The worker's slot and its generation.</param>
    [PgBackgroundWorker(EntryPoint = "worf_exchange_main")]
    internal static void ExchangeMain(nuint mainArg)
    {
        Worker.WorkerAttach(mainArg, WorfWorkerType.Exchange);
        WorfLog.Debug1("starting sequence range exchange worker");
        long started = Worker.GetCurrentTimestamp();
        while (!Worker.GotSigterm && !s_unwatched)
        {
            try
            {
                PgInterrupts.Check();
                Worker.ProcessConfigReload();

                // Backends reload their blocks once the change is committed, and the range worker checks at once, since a
                // block that came may be due, as when the active one ran low before it came.
                if (SequenceRangeExchange.ExchangeAll())
                {
                    SequenceRanges.Changed();
                    _ = PgAtomic.Add(SequenceRanges.CheckRequested, 1L);
                    Worker.WakeRangeWorker();
                }

                // A database that manages nothing has asked its providers, once their answers have come, and the manager
                // looks again later, unless a subscription is still making its slot in the first round, for an exchange
                // interval at most, as one may stay stuck in its initialization.
                if (!SequenceRangeExchange.Pending && (!SequenceRangeExchange.Starting || !Worker.MyWorker.Worker.Exchange.WaitsForSlots ||
                        Worker.GetCurrentTimestamp() - started > SequenceRanges.ExchangeInterval * 1000L) &&
                    !PgBackgroundWorker.RunTransaction(Managed))
                {
                    return;
                }

                SequenceRangeExchange.Wait(SequenceRanges.CheckInterval);
            }
            catch (PgQueryCanceledException)
            {
                // A cancel, as a superuser's, ends only the round, and the exchanges go on.
            }
        }
    }

    /// <summary>
    /// Watches the database's exchange worker, from its range worker or, without one, its manager, and cancels a call
    /// of it that has run out of time, or any call as the watcher stops, which also tells the exchange worker to make no
    /// more, in one step with the call's start, so that none starts after the watcher's last look. A call that waits on a
    /// neighbor that never answers doesn't see the exchange worker's own stop request, so it would hold up a server's
    /// shutdown.
    /// </summary>
    /// <param name="stopping">Whether the watcher stops.</param>
    /// <returns>Whether the exchange worker runs.</returns>
    internal static bool Watch(bool stopping = false)
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
                long deadline = worker.Worker.Exchange.Deadline;
                if (worker.Proc != 0 && deadline > 0 && (stopping || now >= deadline))
                {
                    unsafe
                    {
                        overdue = ((PGPROC*)worker.Proc)->pid;
                    }
                }

                if (stopping || (deadline > 0 && now >= deadline))
                {
                    worker.Worker.Exchange.Deadline = stopping ? Stopped : Cancelling;
                    guard[i] = worker;
                }
            }
        }

        // The call fails in the exchange worker, which tries the neighbor again later. One that has exited, as at a shutdown,
        // is left alone. The cancel sent, a call that ended on its own meanwhile, which waits for it, takes it.
        if (overdue != 0)
        {
            _ = PgBackgroundWorker.RunTransaction(() => Worf.ReadScalar<bool>(
                "SELECT pg_catalog.pg_cancel_backend(pid) FROM pg_catalog.pg_stat_get_activity($1) WHERE pid IS NOT NULL", SpiParameter.Create(overdue)));
        }

        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
        {
            for (int i = 0; i < guard.Length; i++)
            {
                WorfWorker worker = guard[i];
                if (worker.WorkerType == WorfWorkerType.Exchange && worker.Dboid == dboid && worker.Worker.Exchange.Deadline == Cancelling)
                {
                    worker.Worker.Exchange.Deadline = 0;
                    guard[i] = worker;
                }
            }
        }

        return running;
    }

    /// <summary>
    /// Wakes the database's exchange worker, as when a request waits to be sent, from a worker or a backend.
    /// </summary>
    internal static void Wake()
    {
        uint dboid;
        unsafe
        {
            dboid = NativeGlobals.MyDatabaseId;
        }

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
    /// Notes, from the exchange worker, whether its watcher has stopped, after which it makes no more exchanges and stops
    /// too.
    /// </summary>
    internal static void CheckWatcher() => _ = SetDeadline(0);

    /// <summary>
    /// Sets the deadline of the call about to start, or clears it as the call ends, unless the watcher has stopped: a
    /// call doesn't start then.
    /// </summary>
    /// <returns>The deadline it replaced: zero, as a call ends, if the watcher cancelled the call.</returns>
    private static long SetDeadline(long deadline)
    {
        if (Worker.MyWorfWorker < 0)
        {
            return Stopped;
        }

        using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive();
        WorfWorker worker = guard[Worker.MyWorfWorker];
        long replaced = worker.Worker.Exchange.Deadline;
        if (replaced == Cancelling && deadline == 0)
        {
            // The watcher marks the cancel sent itself.
            return replaced;
        }

        if (replaced == Stopped)
        {
            s_unwatched = true;
            if (deadline != 0)
            {
                throw new PgException("57P01", "the sequence range exchange worker is stopping, since its watcher stopped");
            }

            return replaced;
        }

        worker.Worker.Exchange.Deadline = deadline;
        guard[Worker.MyWorfWorker] = worker;
        return replaced;
    }

    private static bool Managed()
        => Worf.ReadScalar<bool>("SELECT to_regclass('worf.sequence_range') IS NOT NULL") &&
            Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range)");
}
