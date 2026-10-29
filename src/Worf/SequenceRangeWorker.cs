
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
                managed = sequences.Count > 0 || Worf.SelectCatalog(
                    "SELECT 1 FROM worf.subscription WHERE sub_target = $1 AND sub_enabled", SpiParameter.Create(node.Node.Id)).Count > 0;
                for (int i = 0; i < sequences.Count; i++)
                {
                    uint seqoid = sequences[i].Get<uint>(0);
                    double rate = Rate(rates, seqoid);
                    try
                    {
                        PgTransaction.RunInSubtransaction(() => changed |= SequenceRanges.Check(seqoid, node, rate));
                    }
                    catch (PgException error)
                    {
                        PgLog.Warning($"could not check managed sequence {seqoid}: {error.Message}");
                    }
                }
            });

            // Requests are committed before the exchanges send them.
            if (managed)
            {
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
                PgLog.Log("no managed sequences or subscriptions left, sequence range worker exiting");
                return;
            }

            _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(SequenceRanges.CheckInterval));
        }
    }

    /// <summary>
    /// Tracks a sequence's insert rate within its active block and returns its peak, which decays while the sequence is
    /// idle. A switch moves the position by more than the sequence issued, so it starts a new sample.
    /// </summary>
    private static double Rate(Dictionary<uint, (long Block, long Position, long At, double Peak)> rates, uint seqoid)
    {
        long now = Worker.GetCurrentTimestamp();
        SpiResult state = Worf.SelectCatalog("""
            SELECT pg_catalog.pg_sequence_last_value($1::regclass), s.seqincrement,
                (SELECT lo FROM worf.sequence_range_block WHERE seqoid = $1 AND kind = 'n' AND state = 'active')
            FROM pg_catalog.pg_sequence s WHERE s.seqrelid = $1
            """, SpiParameter.Create(seqoid));
        bool known = rates.TryGetValue(seqoid, out (long Block, long Position, long At, double Peak) previous);
        if (state.Count == 0 || state[0].Get<long?>(0) is not long position || state[0].Get<long?>(2) is not long block)
        {
            return known ? previous.Peak : 0;
        }

        double peak = known ? previous.Peak : 0;
        if (known && previous.Block == block && now > previous.At)
        {
            double seconds = (now - previous.At) / 1e6;
            double rate = Math.Max(((double)position - previous.Position) / state[0].Get<long>(1), 0) / seconds;
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
