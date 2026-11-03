using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The worker that makes a database's exchanges with its neighbors, apart from its range worker, so that a neighbor that
/// never answers holds up none of the range worker's switches and Snowflake clock reservations. libpq keeps no time
/// limit of its own on a connection a backend waits for, so the range worker, its watcher, cancels any call of it that
/// outlasts <c>worf.sequence_range_timeout</c>. The manager starts both. In a database that manages no sequence, the
/// manager starts it alone, once an exchange interval, and watches it; it exchanges once, to ask the providers whether
/// they manage any, and exits.
/// </summary>
internal static class SequenceRangeExchangeWorker
{
    // The deadline a watcher that stops leaves, which tells the exchange worker to make no more calls and stop too, since
    // none would cancel a call that never ends.
    private const long Stopped = -1;

    // Whether this exchange worker's watcher stopped.
    private static bool s_unwatched;

    /// <summary>
    /// Gets whether this exchange worker's watcher stopped, so that it makes no more calls.
    /// </summary>
    internal static bool Unwatched => s_unwatched;

    /// <summary>
    /// Runs the exchange worker.
    /// </summary>
    /// <param name="mainArg">The worker's slot.</param>
    [PgBackgroundWorker(EntryPoint = "worf_exchange_main")]
    internal static void ExchangeMain(nuint mainArg)
    {
        Worker.WorkerAttach((int)mainArg, WorfWorkerType.Exchange);
        PgLog.Debug1("starting sequence range exchange worker");
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

                // A database that manages nothing has asked its providers, and the manager looks again later, unless a
                // subscription is still making its slot in the first round, for an exchange interval at most, as one may
                // stay stuck in its initialization.
                if ((!SequenceRangeExchange.Starting || !Worker.MyWorker.Worker.Exchange.WaitsForSlots ||
                        Worker.GetCurrentTimestamp() - started > SequenceRanges.ExchangeInterval * 1000L) &&
                    !PgBackgroundWorker.RunTransaction(Managed))
                {
                    return;
                }

                _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(SequenceRanges.CheckInterval));
            }
            catch (PgQueryCanceledException)
            {
                // Its watcher cancelled a call just as it ended, and the exchanges go on.
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
                    worker.Worker.Exchange.Deadline = stopping ? Stopped : 0;
                    guard[i] = worker;
                }
            }
        }

        // The call fails in the exchange worker, which tries the neighbor again later. One that has exited, as at a shutdown,
        // is left alone.
        if (overdue != 0)
        {
            _ = PgBackgroundWorker.RunTransaction(() => Worf.ReadScalar<bool>(
                "SELECT pg_catalog.pg_cancel_backend(pid) FROM pg_catalog.pg_stat_get_activity($1) WHERE pid IS NOT NULL", SpiParameter.Create(overdue)));
        }

        return running;
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
    /// Gets or sets whether the calls about to be made go to a neighbor whose last call ran out of time, which they only
    /// probe for, for a second at most, so that a neighbor that stays frozen holds up the calls to the others that
    /// little, until one answers in time.
    /// </summary>
    internal static bool Probing { get; set; }

    /// <summary>
    /// Makes a call to a neighbor, from the exchange worker, which its watcher cancels once it has taken longer than
    /// <c>worf.sequence_range_timeout</c>, or a second while <see cref="Probing"/>.
    /// </summary>
    /// <typeparam name="T">The call's result.</typeparam>
    /// <param name="call">The call.</param>
    /// <returns>Its result.</returns>
    internal static T Bounded<T>(Func<T> call)
    {
        // A worker asked to stop makes no new call, which its stop request might not reach.
        if (Worker.GotSigterm)
        {
            throw new PgException("57P01", "the sequence range exchange worker is stopping");
        }

        SetDeadline(Worker.GetCurrentTimestamp() + (Probing ? Math.Min(SequenceRanges.ExchangeTimeout, 1000) : SequenceRanges.ExchangeTimeout) * 1000L);
        try
        {
            return call();
        }
        finally
        {
            SetDeadline(0);
        }
    }

    /// <summary>
    /// Sets the deadline of the call about to start, or clears it as the call ends, unless the watcher has stopped: a
    /// call doesn't start then.
    /// </summary>
    private static void SetDeadline(long deadline)
    {
        if (Worker.MyWorfWorker < 0)
        {
            return;
        }

        using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive();
        WorfWorker worker = guard[Worker.MyWorfWorker];
        if (worker.Worker.Exchange.Deadline == Stopped)
        {
            s_unwatched = true;
            if (deadline != 0)
            {
                throw new PgException("57P01", "the sequence range exchange worker is stopping, since its watcher stopped");
            }

            return;
        }

        worker.Worker.Exchange.Deadline = deadline;
        guard[Worker.MyWorfWorker] = worker;
    }

    private static bool Managed()
        => Worf.ReadScalar<bool>("SELECT to_regclass('worf.sequence_range') IS NOT NULL") &&
            Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range)");
}
