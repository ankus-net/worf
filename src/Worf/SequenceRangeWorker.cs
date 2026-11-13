using System.Globalization;
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
    /// <param name="mainArg">The worker's slot and its generation.</param>
    [PgBackgroundWorker(EntryPoint = "worf_range_main")]
    internal static void RangeMain(nuint mainArg)
    {
        Worker.WorkerAttach(mainArg, WorfWorkerType.Range);
        WorfLog.Debug1("starting sequence range worker");
        bool managedAny = false;

        // Each sequence's active block, its position in it and when that was read as its rate's sample began, and its peak
        // rate in values a second, and when that was worked out.
        Dictionary<uint, (long Block, long Position, long At, double Peak, long PeakAt)> rates = [];

        // What each sequence's last full check saw: the shared count of committed range changes, the sequence's position,
        // the count of requests for checks, and when.
        Dictionary<uint, ((long All, long Sequence) Touched, long? Position, long At)> checkedAt = [];

        // What the last tick that looked for dropped sequences and checked this node's identity saw, and when.
        (long Version, long Requested, long At) lookedAt = (-1, -1, 0);

        // The words grants this node made itself want, which the worker writes in a transaction of its own once the
        // grants have committed, at each tick and after each sequence's check.
        long wordsWritten = Worker.WordsWanted;
        void WriteWords()
        {
            long wanted = Worker.WordsWanted;
            if (wanted == wordsWritten)
            {
                return;
            }

            wordsWritten = wanted;
            try
            {
                PgBackgroundWorker.RunTransaction(static () =>
                {
                    if (Worf.CatalogOid("sequence_range") != 0)
                    {
                        SequenceRanges.WakeSubscribers();
                    }
                });
            }
            catch (PgException error)
            {
                WorfLog.Warning($"could not tell this node's subscribers to call it: {error.Message}");
            }
        }

        while (!Worker.GotSigterm)
        {
            PgInterrupts.Check();
            Worker.ProcessConfigReload();
            WriteWords();

            long requested = SequenceRanges.CheckRequested.Value;

            // Dropped sequences are looked for, and this node's identity checked, once a committed change to the ranges or a
            // session asks, and each second for what nothing announces, as a restore.
            long changes = SequenceRanges.Version.Value;
            long started = Worker.GetCurrentTimestamp();
            bool look = changes != lookedAt.Version || requested != lookedAt.Requested || started - lookedAt.At >= 1_000_000;
            if (look)
            {
                lookedAt = (changes, requested, started);
            }

            // The range state of a sequence whose drop has committed goes first, which the DROP leaves to this worker,
            // and so does that of a sequence created again that joined a disabled epoch.
            bool forgot = false;
            try
            {
                forgot = look && PgBackgroundWorker.RunTransaction(() =>
                    Worf.CatalogOid("sequence_range") != 0 && WorfNode.GetLocalNode(false, true) is not null && SequenceRanges.ForgetDropped());
            }
            catch (PgException error)
            {
                WorfLog.Warning($"could not remove the range state of dropped sequences: {error.Message}");
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
                    WorfLog.Log("sequence range worker managing sequences");
                }

                changed |= look && PgBackgroundWorker.RunTransaction(() => SequenceRanges.CheckIdentity(local));
                // A sequence whose switch worker waits to switch it is left to that. Those that statements wait on go first,
                // including those that statements begin to wait on during the tick, which go next, so that their switches
                // wait for no other sequence's check, however many sequences a busy node takes long to check.
                foreach (uint gone in checkedAt.Keys.Where(seqoid => !sequences.Contains(seqoid)).ToList())
                {
                    _ = checkedAt.Remove(gone);
                }

                List<uint> toCheck = [.. sequences.Where(seqoid => !SequenceRangeSwitchWorker.Running(seqoid))];
                while (toCheck.Count > 0)
                {
                    int next = Math.Max(toCheck.FindIndex(seqoid => SequenceRangeGuard.Drawing(seqoid).Waiting > 0), 0);
                    uint seqoid = toCheck[next];
                    toCheck.RemoveAt(next);
                    try
                    {
                        changed |= PgBackgroundWorker.RunTransaction(() =>
                        {
                            // The lock nextval takes keeps a switch from committing between the reads of the sequence's
                            // blocks and of its position. A sequence that a transaction holds against it waits for a later
                            // tick, so that the worker waits on no session.
                            // Its next block comes all the same, which a transaction that switched it itself, and so
                            // holds it against the check, switches to at the block's end, sized by the rate last taken,
                            // since the position can't be read without the lock, and the transaction's draws, as a
                            // load's, don't count toward it.
                            unsafe
                            {
                                if (!NativeMethods.ConditionalLockRelationOid(seqoid, (int)PgLockMode.RowExclusive))
                                {
                                    SequenceRanges.LedgerNoWait = true;
                                    try
                                    {
                                        return SequenceRanges.Replenish(seqoid, local, rates.TryGetValue(seqoid, out (long Block, long Position, long At, double Peak, long PeakAt) last) ? last.Peak : 0);
                                    }
                                    finally
                                    {
                                        SequenceRanges.LedgerNoWait = false;
                                    }
                                }
                            }

                            try
                            {
                                // A sequence that hasn't moved since a check less than ten check intervals ago, and at least a
                                // second, whose ranges no change committed since has touched, which no statement waits on and
                                // no session asked to have checked, has nothing new for the check, which still runs that often
                                // for what time alone changes, as a fence's wait or a rate's decay. Changes to other
                                // sequences' ranges don't count.
                                (long All, long Sequence) touched = SequenceRangeTouches.Touched(seqoid);
                                long? position = Worf.ReadScalar<long?>("SELECT pg_catalog.pg_sequence_last_value($1)", SpiParameter.Create(seqoid));
                                long now = Worker.GetCurrentTimestamp();
                                if (checkedAt.TryGetValue(seqoid, out ((long All, long Sequence) Touched, long? Position, long At) seen) &&
                                    seen.Touched == touched && seen.Position == position &&
                                    now - seen.At < Math.Max(1000L, 10L * SequenceRanges.CheckInterval) * 1000L &&
                                    SequenceRangeGuard.Drawing(seqoid).Waiting == 0)
                                {
                                    return false;
                                }

                                checkedAt[seqoid] = (touched, position, now);
                                WorfLog.Debug2($"sequence range worker checking sequence {seqoid.ToString(CultureInfo.InvariantCulture)} in full");
                                SequenceRanges.LedgerNoWait = true;
                                double rate = Rate(rates, seqoid);
                                // A lift waits only so long for a row another transaction holds, as a drop left open does, and
                                // goes with a later tick, so that the check and every other sequence's go on.
                                try
                                {
                                    PgTransaction.RunInSubtransaction(() => _ = SequenceRangeExchange.WithLockTimeout(() => SequenceRanges.LiftFence(seqoid, local, false)));
                                }
                                catch (PgException held) when (held.SqlState == "55P03")
                                {
                                    WorfLog.Debug2($"sequence range worker leaves sequence {seqoid.ToString(CultureInfo.InvariantCulture)}'s fence to a later tick: {held.Message}");
                                }

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
                            WorfLog.Warning(new PgDiagnostic($"could not check managed sequence {seqoid}: {error.Message}") { Detail = error.Detail });
                        }
                    }

                    SequenceRanges.HandOff(local, seqoid => rates.TryGetValue(seqoid, out (long Block, long Position, long At, double Peak, long PeakAt) last) ? last.Peak : 0);
                    SequenceRangeGuard.CountAttempt(seqoid);
                    WriteWords();
                }

                // Snowflake IDs never pass the time reserved ahead, which backends see only once it is durable.
                try
                {
                    PgBackgroundWorker.RunTransaction(SnowflakeIds.Reserve);
                }
                catch (PgException error)
                {
                    WorfLog.Warning($"could not reserve the Snowflake clock: {error.Message}");
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
                    WorfLog.Log("no managed sequences left, sequence range worker exiting");
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
        WorfRows rows = Worf.SelectCatalog("SELECT r.seqoid::oid FROM worf.sequence_range r WHERE EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.oid = r.seqoid) ORDER BY r.seqoid");
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
        WorfRows state = Worf.SelectCatalog("""
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
