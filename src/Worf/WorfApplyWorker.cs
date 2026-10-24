namespace Worf;

/// <summary>
/// An apply worker's shared state.
/// </summary>
internal struct WorfApplyWorker
{
    /// <summary>
    /// Subscription id for apply worker.
    /// </summary>
    public uint SubId;

    /// <summary>
    /// Is there new synchronization info pending?.
    /// </summary>
    public bool SyncPending;

    /// <summary>
    /// Replay should stop here if defined.
    /// </summary>
    public ulong ReplayStopLsn;
}
