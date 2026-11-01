using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The worker that keeps a database's managed sequences supplied: it switches each to its next block when the active
/// one runs low and gets new blocks, checking every <c>worf.sequence_range_check_interval</c>.
/// </summary>
internal static class SequenceRangeWorker
{
    /// <summary>
    /// Runs the range worker.
    /// </summary>
    /// <param name="mainArg">The worker's slot.</param>
    [PgBackgroundWorker(EntryPoint = "worf_range_main")]
    internal static void RangeMain(nuint mainArg)
    {
        Worker.WorkerAttach((int)mainArg, WorfWorkerType.Range);
        PgLog.Log("starting sequence range worker");

        // Each sequence's active block, its last position in it and when it was read, and its peak rate in values a
        // second.
        Dictionary<uint, (long Block, long Position, long At, double Peak)> rates = [];
        while (!Worker.GotSigterm)
        {
            PgInterrupts.Check();
            Worker.ProcessConfigReload();

            long requested = SequenceRanges.CheckRequested.Value;
            bool managed = false;
            bool changed = false;
            bool received = false;
            PgBackgroundWorker.RunTransaction(() =>
            {
                if (WorfNode.GetLocalNode(false, true) is not WorfLocalNode node)
                {
                    return;
                }

                SpiResult sequences = Worf.SelectCatalog("SELECT seqoid::oid FROM worf.sequence_range");
                managed = sequences.Count > 0;
                if (sequences.Count > 0)
                {
                    changed |= SequenceRanges.CheckIdentity(node);
                }

                for (int i = 0; i < sequences.Count; i++)
                {
                    uint seqoid = sequences[i].Get<uint>(0);
                    try
                    {
                        PgTransaction.RunInSubtransaction(() =>
                        {
                            // The lock nextval takes keeps a switch from committing between the reads of the sequence's
                            // blocks and of its position. A sequence that a transaction holds against it, as a
                            // reservation's switch does until it commits, waits for a later tick, so that the worker
                            // waits on no session.
                            unsafe
                            {
                                if (!NativeMethods.ConditionalLockRelationOid(seqoid, (int)PgLockMode.RowExclusive))
                                {
                                    return;
                                }
                            }

                            try
                            {
                                double rate = Rate(rates, seqoid);
                                _ = SequenceRanges.LiftFence(seqoid, node, false);
                                changed |= SequenceRanges.Check(seqoid, node, rate);
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

                // Snowflake IDs never pass the time reserved ahead.
                if (sequences.Count > 0)
                {
                    try
                    {
                        PgTransaction.RunInSubtransaction(SnowflakeIds.Reserve);
                    }
                    catch (PgException error)
                    {
                        PgLog.Warning($"could not reserve the Snowflake clock: {error.Message}");
                    }
                }
            });

            // Requests are committed before the exchanges send them, including those that sessions handed over.
            if (managed)
            {
                List<Guid> recorded = PgBackgroundWorker.RunTransaction(() =>
                    WorfNode.GetLocalNode(false, true) is WorfLocalNode node ? SequenceRangeReservations.Record(node) : []);
                SequenceRangeReservations.Forget(recorded);
                changed |= recorded.Count > 0;
                PgBackgroundWorker.RunTransaction(() =>
                {
                    if (WorfNode.GetLocalNode(false, true) is WorfLocalNode node)
                    {
                        received = SequenceRangeExchange.ExchangeAll(node);
                    }
                });
                changed |= received;
            }

            // Backends reload their blocks once the change is committed.
            if (changed)
            {
                SequenceRanges.Changed();
            }

            if (!managed)
            {
                PgLog.Log("no managed sequences left, sequence range worker exiting");
                return;
            }

            // A block that an exchange brought may be due at once, as when the active one ran low before it came, and so
            // may a switch that a session woke the worker for during an exchange, whose connection's waits reset the
            // latch.
            if (!received && SequenceRanges.CheckRequested.Value == requested)
            {
                _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(SequenceRanges.CheckInterval));
            }
        }
    }

    /// <summary>
    /// Tracks a sequence's insert rate within its active block and returns its peak, which decays while the sequence is
    /// idle. A switch moves the position by more than the sequence issued, so it starts a new sample, from the block's
    /// start as it became active, so that a block that runs out before the next check still shows its rate. A block that
    /// a reservation switched to, which carries no activation time, serves a load rather than the usual inserts, and isn't
    /// sampled.
    /// </summary>
    private static double Rate(Dictionary<uint, (long Block, long Position, long At, double Peak)> rates, uint seqoid)
    {
        long now = Worker.GetCurrentTimestamp();
        SpiResult state = Worf.SelectCatalog("""
            SELECT s.seqincrement, b.lo, CASE WHEN s.seqincrement > 0 THEN b.lo ELSE b.hi END,
                extract(epoch FROM clock_timestamp() - b.activated_at)::float8
            FROM pg_catalog.pg_sequence s
                LEFT JOIN worf.sequence_range_block b
                    ON b.seqoid = s.seqrelid AND b.kind = 'n' AND b.state = 'active' AND b.activated_at IS NOT NULL
            WHERE s.seqrelid = $1
            """, SpiParameter.Create(seqoid));
        bool known = rates.TryGetValue(seqoid, out (long Block, long Position, long At, double Peak) previous);
        if (state.Count == 0 || state[0].Get<long?>(1) is not long block)
        {
            return known ? previous.Peak : 0;
        }

        // Before its first value, the sequence stands a step before its block's start.
        long increment = state[0].Get<long>(0);
        long start = state[0].Get<long>(2);
        long position = SequenceRanges.LastValue(seqoid) ?? start - increment;
        double peak = known ? previous.Peak : 0;
        if (!known || previous.Block != block)
        {
            previous = (block, start - increment, now - (long)(state[0].Get<double>(3) * 1e6), peak);
            rates[seqoid] = previous;
        }

        double seconds = (now - previous.At) / 1e6;
        double rate = seconds > 0 ? Math.Max(((double)position - previous.Position) / increment, 0) / seconds : 0;

        // A tick that comes early, as when a session wakes the worker, leaves the sample running, so that a burst isn't
        // taken over a moment as the peak. The rate so far still counts for this check, so that a sequence that runs
        // through its block faster than the worker checks switches in time, to a block sized for that rate.
        if (now - previous.At < SequenceRanges.CheckInterval * 1000L)
        {
            return Math.Max(peak, rate);
        }

        peak = Math.Max(rate, peak * Math.Pow(0.99, seconds));
        if (Math.Abs(peak - previous.Peak) > Math.Max(previous.Peak * 0.1, 1))
        {
            _ = Spi.Execute("UPDATE worf.sequence_range SET peak_rate = $2 WHERE seqoid = $1", SpiParameter.Create(seqoid),
                SpiParameter.Create(peak));
        }

        rates[seqoid] = (block, position, now, peak);
        return peak;
    }
}
