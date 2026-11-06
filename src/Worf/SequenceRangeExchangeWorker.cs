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

    // The deadline the watcher leaves on a call it cancels until it has sent the cancel, which the call, if it ended on
    // its own meanwhile, waits for.
    private const long Cancelling = -2;

    // Whether this exchange worker's watcher stopped.
    private static bool s_unwatched;

    // Whether the watcher cancelled a call that ended before the cancel came, which is still to come.
    private static bool s_cancelOwed;

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
                _ = TakeOwedCancel();
            }
        }
    }

    /// <summary>
    /// Watches the database's exchange worker, from its range worker or, without one, its manager, and cancels a call
    /// of it that has run out of time, or any call as the watcher stops, which also tells the exchange worker to make no
    /// more, in one step with the call's start, so that none starts after the watcher's last look. A call that waits on a
    /// neighbor that never answers doesn't see the exchange worker's own stop request, so it would hold up a server's
    /// shutdown. A probe of a neighbor whose last call ran out of time gives way as soon as a request waits on another
    /// neighbor, which the range worker looks for right after it makes one, so that a frozen neighbor holds up no
    /// block this node asks another for.
    /// </summary>
    /// <param name="stopping">Whether the watcher stops.</param>
    /// <returns>Whether the exchange worker runs.</returns>
    internal static bool Watch(bool stopping = false)
    {
        uint dboid = Worker.MyWorker.Dboid;
        long now = Worker.GetCurrentTimestamp();
        bool running = false;
        int overdue = 0;
        (long Deadline, uint Node, long StartedAt) probe = default;
        using (PgLwLockShareGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Share())
        {
            for (int i = 0; i < guard.Length && !stopping; i++)
            {
                WorfExchangeWorker exchange = guard[i].Worker.Exchange;
                if (guard[i].WorkerType == WorfWorkerType.Exchange && guard[i].Dboid == dboid && guard[i].CrashedAt == 0 &&
                    guard[i].Proc != 0 && exchange.Deadline > now && exchange.Probing != 0)
                {
                    probe = (exchange.Deadline, exchange.Probing, exchange.StartedAt);
                }
            }
        }

        // A probe gives way to a request on another node made within the exchange timeout, which that node is about to
        // answer, but not to one that has stayed open longer, as one the node can't answer, so that the once-a-minute probe
        // of a neighbor doesn't give way for as long as such a request stays open.
        bool giveWay = probe.Node != 0 && PgBackgroundWorker.RunTransaction(() => Worf.ReadScalar<bool>("""
            SELECT EXISTS (
                SELECT 1 FROM worf.sequence_range_request
                WHERE state = 'open' AND parent_node <> $1 AND created_at >= least($2, pg_catalog.clock_timestamp() - $3 * interval '1 millisecond'))
            """, SpiParameter.Create(probe.Node), SpiParameter.Create(new PgTimestampTz(probe.StartedAt)), SpiParameter.Create(SequenceRanges.ExchangeTimeout)));
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
                bool givingWay = giveWay && deadline == probe.Deadline && worker.Worker.Exchange.Probing == probe.Node;
                if (worker.Proc != 0 && deadline > 0 && (stopping || now >= deadline || givingWay))
                {
                    unsafe
                    {
                        overdue = ((PGPROC*)worker.Proc)->pid;
                    }
                }

                if (stopping || (deadline > 0 && now >= deadline) || givingWay)
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
    /// Gets or sets the node the calls about to be made go to, if its last call ran out of time, which they only probe
    /// for a second at most, three for the connection, and give way to a request that waits on another node, so that a
    /// neighbor that stays frozen holds up the calls to the others that little, until one answers in time; or zero.
    /// </summary>
    internal static uint Probing { get; set; }

    /// <summary>
    /// Gets or sets whether a probe gives way to a request that waits on another node. One that gave way last time
    /// doesn't, so that a neighbor slow to answer is reached though requests on the others keep coming.
    /// </summary>
    internal static bool Yields { get; set; } = true;

    /// <summary>
    /// Gets how long, in milliseconds, the last call that started had.
    /// </summary>
    internal static int LastLimit { get; private set; }

    /// <summary>
    /// Gets how long, in milliseconds, a call about to start has: <c>worf.sequence_range_timeout</c>, or, as a probe,
    /// a second, or three for the connection, which takes a few round trips more.
    /// </summary>
    /// <param name="connecting">Whether the call makes the connection to the neighbor.</param>
    /// <returns>The limit.</returns>
    internal static int CallLimit(bool connecting)
        => Probing != 0 ? Math.Min(SequenceRanges.ExchangeTimeout, connecting ? 3000 : 1000) : SequenceRanges.ExchangeTimeout;

    /// <summary>
    /// Makes a call to a neighbor, from the exchange worker, which its watcher cancels once it has taken longer than
    /// <c>worf.sequence_range_timeout</c>, or, as a probe, a second, or as soon as a request waits on another node.
    /// </summary>
    /// <typeparam name="T">The call's result.</typeparam>
    /// <param name="call">The call.</param>
    /// <param name="connecting">Whether the call makes the connection to the neighbor.</param>
    /// <returns>Its result.</returns>
    internal static T Bounded<T>(Func<T> call, bool connecting = false)
    {
        // A worker asked to stop makes no new call, which its stop request might not reach.
        if (Worker.GotSigterm)
        {
            throw new PgException("57P01", "the sequence range exchange worker is stopping");
        }

        LastLimit = CallLimit(connecting);
        long deadline = Worker.GetCurrentTimestamp() + (LastLimit * 1000L);
        _ = SetDeadline(deadline);
        bool canceled = false;
        try
        {
            return call();
        }
        catch (PgQueryCanceledException)
        {
            canceled = true;
            throw;
        }
        finally
        {
            // A call the watcher cancelled that ended first, as at its own time limit, which is the watcher's too, takes
            // the cancel that comes after it, so that it lands in no later call, nor in the work between calls.
            if (SetDeadline(0) is 0 or Cancelling && !canceled)
            {
                AwaitCancel();
            }
        }
    }

    /// <summary>
    /// Waits for the cancel of a call that the watcher cancelled as it ended on its own, and takes it: until the watcher
    /// has sent it and it has come, or, should the watcher fail first, for <c>worf.sequence_range_timeout</c>, after
    /// which the next cancel that comes is taken for it. In a transaction, the cancel is taken in a subtransaction that
    /// it rolls back, as an exception block takes an error.
    /// </summary>
    private static void AwaitCancel()
    {
        bool inTransaction;
        unsafe
        {
            inTransaction = NativeMethods.IsTransactionState();
        }

        long started = Worker.GetCurrentTimestamp();
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
                return;
            }

            long now = Worker.GetCurrentTimestamp();
            if (sentAt == 0 && Deadline() != Cancelling)
            {
                sentAt = now;
            }

            if ((sentAt != 0 && now - sentAt >= 1_000_000) || now - started >= SequenceRanges.ExchangeTimeout * 1000L)
            {
                s_cancelOwed = true;
                return;
            }

            Worker.WaitLatch(10);
        }
    }

    /// <summary>
    /// Reads the deadline the exchange worker's slot holds.
    /// </summary>
    private static long Deadline()
    {
        using PgLwLockShareGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Share();
        return guard[Worker.MyWorfWorker].Worker.Exchange.Deadline;
    }

    /// <summary>
    /// Takes a cancel the watcher sent for a call that had already ended, as the one that just came: the call it came
    /// during, or the exchange, didn't run out of time, and is made again.
    /// </summary>
    /// <returns>Whether a cancel was owed.</returns>
    internal static bool TakeOwedCancel()
    {
        bool owed = s_cancelOwed;
        s_cancelOwed = false;
        return owed;
    }

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
            worker.Worker.Exchange.Probing = 0;
            worker.Worker.Exchange.StartedAt = 0;
            guard[Worker.MyWorfWorker] = worker;
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
        worker.Worker.Exchange.Probing = deadline != 0 && Yields ? Probing : 0;
        worker.Worker.Exchange.StartedAt = deadline != 0 ? Worker.GetCurrentTimestamp() : 0;
        guard[Worker.MyWorfWorker] = worker;
        return replaced;
    }

    private static bool Managed()
        => Worf.ReadScalar<bool>("SELECT to_regclass('worf.sequence_range') IS NOT NULL") &&
            Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range)");
}
