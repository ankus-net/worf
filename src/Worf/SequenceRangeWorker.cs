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

            bool managed = false;
            bool changed = false;
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
                        changed |= SequenceRangeExchange.ExchangeAll(node);
                    }
                });
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

            _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(SequenceRanges.CheckInterval));
        }
    }

    /// <summary>
    /// Tracks a sequence's insert rate within its active block and returns its peak, which decays while the sequence is
    /// idle. A switch moves the position by more than the sequence issued, so it starts a new sample. A block that a
    /// reservation switched to, which carries no activation time, serves a load rather than the usual inserts, and isn't
    /// sampled.
    /// </summary>
    private static double Rate(Dictionary<uint, (long Block, long Position, long At, double Peak)> rates, uint seqoid)
    {
        long now = Worker.GetCurrentTimestamp();
        SpiResult state = Worf.SelectCatalog("""
            SELECT s.seqincrement,
                (SELECT lo FROM worf.sequence_range_block WHERE seqoid = $1 AND kind = 'n' AND state = 'active' AND activated_at IS NOT NULL)
            FROM pg_catalog.pg_sequence s WHERE s.seqrelid = $1
            """, SpiParameter.Create(seqoid));
        bool known = rates.TryGetValue(seqoid, out (long Block, long Position, long At, double Peak) previous);
        if (state.Count == 0 || SequenceRanges.LastValue(seqoid) is not long position || state[0].Get<long?>(1) is not long block)
        {
            return known ? previous.Peak : 0;
        }

        double peak = known ? previous.Peak : 0;
        if (known && previous.Block == block && now > previous.At)
        {
            double seconds = (now - previous.At) / 1e6;
            double rate = Math.Max(((double)position - previous.Position) / state[0].Get<long>(0), 0) / seconds;
            peak = Math.Max(rate, previous.Peak * Math.Pow(0.99, seconds));
            if (Math.Abs(peak - previous.Peak) > Math.Max(previous.Peak * 0.1, 1))
            {
                _ = Spi.Execute("UPDATE worf.sequence_range SET peak_rate = $2 WHERE seqoid = $1", SpiParameter.Create(seqoid),
                    SpiParameter.Create(peak));
            }
        }

        rates[seqoid] = (block, position, now, peak);
        return peak;
    }
}
