using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The worker that keeps a database's managed sequences supplied: it switches each to its next block when the active
/// one runs low and gets new blocks, checking every <c>worf.sequence_range_check_interval</c>. Its exchange worker
/// makes the calls to the neighbors. A tick that changes nothing writes nothing, so an idle node uses no transaction
/// IDs.
/// </summary>
internal static class SequenceRangeWorker
{
    // The shortest stretch, in microseconds, that a sequence's insert rate is taken over.
    private const long RateSample = 10_000_000;

    /// <summary>
    /// Runs the range worker.
    /// </summary>
    /// <param name="mainArg">The worker's slot.</param>
    [PgBackgroundWorker(EntryPoint = "worf_range_main")]
    internal static void RangeMain(nuint mainArg)
    {
        Worker.WorkerAttach((int)mainArg, WorfWorkerType.Range);
        PgLog.Debug1("starting sequence range worker");
        bool managedAny = false;

        // Each sequence's active block, its position in it and when that was read as its rate's sample began, and its peak
        // rate in values a second, and when that was worked out.
        Dictionary<uint, (long Block, long Position, long At, double Peak, long PeakAt)> rates = [];
        while (!Worker.GotSigterm)
        {
            PgInterrupts.Check();
            Worker.ProcessConfigReload();

            long requested = SequenceRanges.CheckRequested.Value;

            // The range state of a sequence whose drop has committed goes first, which the DROP leaves to this worker,
            // and so does that of a sequence created again that joined a disabled epoch.
            bool forgot = false;
            try
            {
                forgot = PgBackgroundWorker.RunTransaction(() =>
                    Worf.CatalogOid("sequence_range") != 0 && WorfNode.GetLocalNode(false, true) is not null && SequenceRanges.ForgetDropped());
            }
            catch (PgException error)
            {
                PgLog.Warning($"could not remove the range state of dropped sequences: {error.Message}");
            }

            // Each sequence is checked in a transaction of its own, so that its locks are held no longer than its check,
            // which an exchange answered meanwhile may wait for.
            (WorfLocalNode Node, List<uint> Sequences)? managed = PgBackgroundWorker.RunTransaction(() =>
                WorfNode.GetLocalNode(false, true) is WorfLocalNode node ? (node, Sequences()) : ((WorfLocalNode, List<uint>)?)null);
            bool changed = forgot;
            if (managed is (WorfLocalNode local, List<uint> sequences) && sequences.Count > 0)
            {
                if (!managedAny)
                {
                    managedAny = true;
                    PgLog.Log("sequence range worker managing sequences");
                }

                changed |= PgBackgroundWorker.RunTransaction(() => SequenceRanges.CheckIdentity(local));
                // A sequence whose switch worker waits to switch it is left to that.
                foreach (uint seqoid in sequences.Where(seqoid => !SequenceRangeSwitchWorker.Running(seqoid)))
                {
                    try
                    {
                        changed |= PgBackgroundWorker.RunTransaction(() =>
                        {
                            // The lock nextval takes keeps a switch from committing between the reads of the sequence's
                            // blocks and of its position. A sequence that a transaction holds against it waits for a later
                            // tick, so that the worker waits on no session.
                            unsafe
                            {
                                if (!NativeMethods.ConditionalLockRelationOid(seqoid, (int)PgLockMode.RowExclusive))
                                {
                                    return false;
                                }
                            }

                            try
                            {
                                SequenceRanges.LedgerNoWait = true;
                                double rate = Rate(rates, seqoid);
                                _ = SequenceRanges.LiftFence(seqoid, local, false);
                                return SequenceRanges.Check(seqoid, local, rate);
                            }
                            finally
                            {
                                SequenceRanges.LedgerNoWait = false;
                                unsafe
                                {
                                    NativeMethods.UnlockRelationOid(seqoid, (int)PgLockMode.RowExclusive);
                                }
                            }
                        });
                    }
                    catch (PgException error)
                    {
                        // A sequence dropped since the tick began, or whose range a transaction holds, goes with the next
                        // tick.
                        if (error.SqlState != "55P03" && PgBackgroundWorker.RunTransaction(() => SequenceRanges.SequenceExists(seqoid)))
                        {
                            PgLog.Warning(new PgDiagnostic($"could not check managed sequence {seqoid}: {error.Message}") { Detail = error.Detail });
                        }
                    }

                    SequenceRanges.HandOff();
                }

                // Snowflake IDs never pass the time reserved ahead, which backends see only once it is durable.
                try
                {
                    PgBackgroundWorker.RunTransaction(SnowflakeIds.Reserve);
                }
                catch (PgException error)
                {
                    PgLog.Warning($"could not reserve the Snowflake clock: {error.Message}");
                }

                // Requests are committed before the exchange worker sends them, including those that sessions handed over.
                List<Guid> recorded = PgBackgroundWorker.RunTransaction(() => SequenceRangeReservations.Record(local));
                SequenceRangeReservations.Forget(recorded);
                changed |= recorded.Count > 0;
            }

            // Backends reload their blocks once the change is committed, and the exchange worker sends what is waiting.
            if (changed)
            {
                SequenceRanges.Changed();
                SequenceRangeExchangeWorker.Wake();
            }

            // Once the database manages no sequence, this worker watches the exchange worker until it is done.
            bool exchanging = managed is not null && SequenceRangeExchangeWorker.Watch();
            if (managed is null || (!exchanging && PgBackgroundWorker.RunTransaction(Sequences).Count == 0))
            {
                if (managedAny)
                {
                    PgLog.Log("no managed sequences left, sequence range worker exiting");
                }

                return;
            }

            // A session that woke the worker while it waited, as on a lock, which resets its latch, or an exchange that
            // brought a block, has it check again at once.
            if (SequenceRanges.CheckRequested.Value == requested)
            {
                _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(SequenceRanges.CheckInterval));
            }
        }

        _ = SequenceRangeExchangeWorker.Watch(true);
    }

    private static List<uint> Sequences()
    {
        SpiResult rows = Worf.SelectCatalog("SELECT r.seqoid::oid FROM worf.sequence_range r WHERE EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.oid = r.seqoid) ORDER BY r.seqoid");
        List<uint> sequences = [];
        for (int i = 0; i < rows.Count; i++)
        {
            sequences.Add(rows[i].Get<uint>(0));
        }

        return sequences;
    }

    /// <summary>
    /// Tracks a sequence's insert rate within its active block and returns its peak, which decays while the sequence is
    /// idle. The rate is the values issued since the sample began, over ten seconds at least, so that a burst isn't taken
    /// for a rate kept up for the two minutes a block is sized for: a sample keeps running until it has run that long, and
    /// then starts again. A switch moves the position by more than the sequence issued, so a new block starts a new
    /// sample, from its start as it became active, so that a block that runs low before the next check still shows the
    /// values it gave, and the peak keeps them though the check switches away from the block. A tick that comes early, as
    /// when a session wakes the worker, takes the rate as any other, since the ten seconds keep a burst from counting for
    /// more than it gave. A block that a reservation switched to, which carries no activation time, serves a load rather
    /// than the usual inserts, and isn't sampled.
    /// </summary>
    private static double Rate(Dictionary<uint, (long Block, long Position, long At, double Peak, long PeakAt)> rates, uint seqoid)
    {
        long now = Worker.GetCurrentTimestamp();
        SpiResult state = Worf.SelectCatalog("""
            SELECT s.seqincrement, b.lo, CASE WHEN s.seqincrement > 0 THEN b.lo ELSE b.hi END,
                extract(epoch FROM clock_timestamp() - b.activated_at)::float8,
                (SELECT r.peak_rate FROM worf.sequence_range r WHERE r.seqoid = s.seqrelid)
            FROM pg_catalog.pg_sequence s
                LEFT JOIN worf.sequence_range_block b
                    ON b.seqoid = s.seqrelid AND b.kind = 'n' AND b.state = 'active' AND b.activated_at IS NOT NULL
            WHERE s.seqrelid = $1
            """, SpiParameter.Create(seqoid));
        bool known = rates.TryGetValue(seqoid, out (long Block, long Position, long At, double Peak, long PeakAt) previous);
        if (state.Count == 0 || state[0].Get<long?>(1) is not long block)
        {
            return known ? previous.Peak : 0;
        }

        // Before its first value, the sequence stands a step before its block's start.
        long increment = state[0].Get<long>(0);
        long first = state[0].Get<long>(2);
        long position = SequenceRanges.LastValue(seqoid) ?? first - increment;
        if (!known || previous.Block != block)
        {
            previous = (block, first - increment, now - (long)(Math.Max(state[0].Get<double>(3), 0) * 1e6), known ? previous.Peak : 0,
                known ? previous.PeakAt : now);
            rates[seqoid] = previous;
        }

        double seconds = Math.Max(now - previous.At, 0) / 1e6;
        double rate = Math.Max(((double)position - previous.Position) / increment, 0) / Math.Max(seconds, RateSample / 1e6);
        // The peak is written once it rises a tenth above the one the range row holds, however small each check's step,
        // and not as it decays, so that an idle sequence writes nothing.
        double peak = Math.Max(rate, previous.Peak * Math.Pow(0.99, (now - previous.PeakAt) / 1e6));
        double written = state[0].Get<double?>(4) ?? 0;
        if (peak - written > Math.Max(written * 0.1, 1))
        {
            SequenceRanges.LockLedger(seqoid);
            _ = Worf.Execute("UPDATE worf.sequence_range SET peak_rate = $2 WHERE seqoid = $1", SpiParameter.Create(seqoid),
                SpiParameter.Create(peak));
        }

        rates[seqoid] = seconds < RateSample / 1e6 ? (block, previous.Position, previous.At, peak, now) : (block, position, now, peak, now);
        return peak;
    }
}
