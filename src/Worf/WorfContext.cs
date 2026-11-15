namespace Worf;

/// <summary>
/// The shared state that the worker slots follow.
/// </summary>
internal struct WorfContext
{
    /// <summary>
    /// Supervisor process.
    /// </summary>
    public nint Supervisor;

    /// <summary>
    /// Signal that subscription info have changed.
    /// </summary>
    public bool SubscriptionsChanged;

    /// <summary>
    /// Background workers.
    /// </summary>
    public int TotalWorkers;

    /// <summary>
    /// When a worker last found no replication origin slot free, for others to give up the slots of relay origins they
    /// no longer need.
    /// </summary>
    public long OriginSlotWantedAt;

    /// <summary>
    /// When a worker last found no replication origin slot free for its subscription's or table sync's own origin, for
    /// others to give up even the relay origins they used last.
    /// </summary>
    public long OwnOriginSlotWantedAt;

    public long OriginSlotsGivenUpAt;

    /// <summary>
    /// When a transaction that may have given a database something to manage, as CREATE EXTENSION worf, last committed
    /// and forgot that managers had nothing to manage, so that a manager that looked before it, and may not have seen
    /// it, isn't taken for idle as it exits.
    /// </summary>
    public long IdleForgottenAt;
}
