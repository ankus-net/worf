namespace Worf;

/// <summary>
/// The worker that switches one managed sequence to its next block when transactions hold it at the active block's end,
/// or as a load waits for its reserved block.
/// It waits for them as long as they hold it, while the range worker goes on checking the other sequences. Inserts that
/// draw from the sequence wait behind it meanwhile, rather than run the block out.
/// </summary>
internal static class SequenceRangeSwitchWorker
{
    /// <summary>
    /// Runs the switch worker.
    /// </summary>
    /// <param name="mainArg">The worker's slot.</param>
    [PgBackgroundWorker(EntryPoint = "worf_switch_main")]
    internal static void SwitchMain(nuint mainArg)
    {
        Worker.WorkerAttach((int)mainArg, WorfWorkerType.Switch);
        uint seqoid = Worker.MyWorker.Worker.Switch.Seqoid;
        PgLog.Debug1($"starting sequence switch worker for sequence {seqoid}");
        while (!Worker.GotSigterm)
        {
            PgInterrupts.Check();
            Worker.ProcessConfigReload();
            try
            {
                if (PgBackgroundWorker.RunTransaction(() => SequenceRanges.SwitchAtEnd(seqoid)))
                {
                    break;
                }
            }
            catch (PgException error) when (error.SqlState is "55P03" or "40P01")
            {
                // A transaction held the range row, as a reset does, or the wait deadlocked, and the worker waits again.
                _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(10));
            }
        }

        // Backends reload their blocks once the switch is committed, and the range worker checks the sequence again.
        SequenceRanges.Changed();
        _ = PgAtomic.Add(SequenceRanges.CheckRequested, 1L);
        Worker.WakeRangeWorker();
    }

    /// <summary>
    /// Starts a switch worker for a sequence, unless one runs for it.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <returns>Whether one runs, or false when every worker slot is taken.</returns>
    internal static bool Start(uint seqoid)
    {
        if (Running(seqoid))
        {
            return true;
        }

        try
        {
            _ = Worker.WorkerRegister(new WorfWorker
            {
                WorkerType = WorfWorkerType.Switch,
                Dboid = Worker.MyWorker.Dboid,
                Worker = new WorfWorkerInfo { Switch = new WorfSwitchWorker { Seqoid = seqoid } },
            });
            return true;
        }
        catch (PgException error)
        {
            PgLog.Warning($"could not start the sequence switch worker for sequence {seqoid}: {error.Message}");
            return false;
        }
    }

    /// <summary>
    /// Finds whether a switch worker runs for a sequence.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <returns>Whether one runs.</returns>
    internal static bool Running(uint seqoid)
    {
        uint dboid = Worker.MyWorker.Dboid;
        using PgLwLockShareGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].WorkerType == WorfWorkerType.Switch && guard[i].Dboid == dboid && guard[i].CrashedAt == 0 &&
                guard[i].Worker.Switch.Seqoid == seqoid)
            {
                return true;
            }
        }

        return false;
    }
}
