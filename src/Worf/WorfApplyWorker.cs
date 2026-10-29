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
    /// The node the worker's subscription applies changes from.
    /// </summary>
    public uint OriginNodeId;

    /// <summary>
    /// How far the worker has applied the provider's WAL, as it reports to the provider: every transaction that
    /// committed before it is applied and flushed here.
    /// </summary>
    public ulong FlushLsn;

    /// <summary>
    /// The commit position of a remote transaction that failed, which the next apply worker applies again under
    /// <c>worf.exception_behaviour</c>, or zero.
    /// </summary>
    public ulong ExceptionLsn;

    /// <summary>
    /// How that transaction is applied: change by change, as a whole in a subtransaction, or not at all.
    /// </summary>
    public int ExceptionLevel;

    /// <summary>
    /// Whether the worker stopped to apply that transaction in a new way, which starts at once rather than after the
    /// delay before restarting a worker that failed.
    /// </summary>
    public bool ExceptionRestartsAtOnce;
}
