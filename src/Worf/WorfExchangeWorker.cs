namespace Worf;

/// <summary>
/// An exchange worker's shared state.
/// </summary>
internal struct WorfExchangeWorker
{
    /// <summary>
    /// When the call the worker waits on runs out of time, or zero while it waits on none.
    /// </summary>
    public long Deadline;

    /// <summary>
    /// Whether the worker of a database that manages nothing, which asks its providers whether they manage any, stays up
    /// while a subscription is still making its slot: in the first round after the manager starts, or after a backend
    /// learned of a sequence, but not in the rounds after, which one stuck in its initialization would keep it up for.
    /// </summary>
    public bool WaitsForSlots;
}
