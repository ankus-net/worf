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
}
