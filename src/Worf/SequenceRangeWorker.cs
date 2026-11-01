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

            // Each sequence is checked in a transaction of its own, so that its locks are held no longer than its check,
            // which an exchange answered meanwhile may wait for.
            (WorfLocalNode Node, List<uint> Sequences)? managed = PgBackgroundWorker.RunTransaction(() =>
                WorfNode.GetLocalNode(false, true) is WorfLocalNode node ? (node, Sequences()) : ((WorfLocalNode, List<uint>)?)null);
            bool changed = false;
            if (managed is (WorfLocalNode local, List<uint> sequences) && sequences.Count > 0)
            {
                if (!managedAny)
                {
                    managedAny = true;
                    PgLog.Log("sequence range worker managing sequences");
                }

                changed |= PgBackgroundWorker.RunTransaction(() => SequenceRanges.CheckIdentity(local));
                foreach (uint seqoid in sequences)
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
                                double rate = Rate(rates, seqoid);
                                _ = SequenceRanges.LiftFence(seqoid, local, false);
                                return SequenceRanges.Check(seqoid, local, rate);
                            }
                            finally
                            {
                                unsafe
                                {
                                    NativeMethods.UnlockRelationOid(seqoid, (int)PgLockMode.RowExclusive);
                                }
                            }
                        });
                    }
                    catch (PgException error)
                    {
                        PgLog.Warning($"could not check managed sequence {seqoid}: {error.Message}");
                    }
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

            _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(SequenceRanges.CheckInterval));
        }

        _ = SequenceRangeExchangeWorker.Watch(true);
    }

    private static List<uint> Sequences()
    {
        SpiResult rows = Worf.SelectCatalog("SELECT seqoid::oid FROM worf.sequence_range ORDER BY seqoid");
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
    /// then starts again. A switch moves the position by more than the sequence issued, so it starts a new sample. A
    /// block that a reservation switched to, which carries no activation time, serves a load rather than the usual
    /// inserts, and isn't sampled.
    /// </summary>
    private static double Rate(Dictionary<uint, (long Block, long Position, long At, double Peak, long PeakAt)> rates, uint seqoid)
    {
        long now = Worker.GetCurrentTimestamp();
        SpiResult state = Worf.SelectCatalog("""
            SELECT s.seqincrement,
                (SELECT lo FROM worf.sequence_range_block WHERE seqoid = $1 AND kind = 'n' AND state = 'active' AND activated_at IS NOT NULL)
            FROM pg_catalog.pg_sequence s WHERE s.seqrelid = $1
            """, SpiParameter.Create(seqoid));
        bool known = rates.TryGetValue(seqoid, out (long Block, long Position, long At, double Peak, long PeakAt) previous);
        if (state.Count == 0 || SequenceRanges.LastValue(seqoid) is not long position || state[0].Get<long?>(1) is not long block)
        {
            return known ? previous.Peak : 0;
        }

        // A tick that comes before a check interval has passed, as when a session wakes the worker, leaves the peak as it
        // was.
        bool sampling = known && previous.Block == block;
        if (sampling && now - previous.PeakAt < SequenceRanges.CheckInterval * 1000L)
        {
            return previous.Peak;
        }

        double peak = known ? previous.Peak * Math.Pow(0.99, Math.Max(now - previous.PeakAt, 0) / 1e6) : 0;
        (long start, long startAt) = (position, now);
        if (sampling && now > previous.At)
        {
            double seconds = (now - previous.At) / 1e6;
            double rate = Math.Max(((double)position - previous.Position) / state[0].Get<long>(0), 0) / Math.Max(seconds, RateSample / 1e6);
            peak = Math.Max(rate, peak);
            if (seconds < RateSample / 1e6)
            {
                (start, startAt) = (previous.Position, previous.At);
            }

            if (Math.Abs(peak - previous.Peak) > Math.Max(previous.Peak * 0.1, 1))
            {
                _ = Spi.Execute("UPDATE worf.sequence_range SET peak_rate = $2 WHERE seqoid = $1", SpiParameter.Create(seqoid),
                    SpiParameter.Create(peak));
            }
        }

        rates[seqoid] = (block, start, startAt, peak, now);
        return peak;
    }
}
