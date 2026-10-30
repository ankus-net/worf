namespace Worf;

/// <summary>
/// The worker that keeps a database's managed sequences supplied: it switches each to its next block when the active
/// one runs low and gets new blocks, checking every <c>worf.sequence_range_check_interval</c>. Its exchange worker
/// makes the calls to the neighbors. In a database that manages none, the manager runs it once an exchange interval to
/// have the exchange worker ask the providers whether they manage any. A tick that changes nothing writes nothing, so
/// an idle node uses no transaction IDs.
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
        PgLog.Debug1("starting sequence range worker");
        bool managedAny = false;

        // Each sequence's active block, its last position in it and when it was read, and its peak rate in values a
        // second.
        Dictionary<uint, (long Block, long Position, long At, double Peak)> rates = [];
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
                            double rate = Rate(rates, seqoid);
                            _ = SequenceRanges.LiftFence(seqoid, local, false);
                            return SequenceRanges.Check(seqoid, local, rate);
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

            // The exchange worker runs while the database manages sequences. A database that manages nothing yet runs it
            // once, to hear of the sequences its providers manage, and this worker waits for it.
            bool exchanging = managed is (_, List<uint> current) && SequenceRangeExchangeWorker.Watch(current.Count > 0);
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
    /// idle. A switch moves the position by more than the sequence issued, so it starts a new sample.
    /// </summary>
    private static double Rate(Dictionary<uint, (long Block, long Position, long At, double Peak)> rates, uint seqoid)
    {
        long now = Worker.GetCurrentTimestamp();
        SpiResult state = Worf.SelectCatalog("""
            SELECT s.seqincrement, (SELECT lo FROM worf.sequence_range_block WHERE seqoid = $1 AND kind = 'n' AND state = 'active')
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
