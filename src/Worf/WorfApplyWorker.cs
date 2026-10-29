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

    /// <summary>
    /// The commit position of a remote transaction that failed, which the next apply worker applies again under
    /// <c>worf.exception_behaviour</c>, or zero.
    /// </summary>
    public ulong ExceptionLsn;

    /// <summary>
    /// Whether that transaction is skipped as a whole, rather than change by change.
    /// </summary>
    public bool ExceptionSkipsTransaction;
}
