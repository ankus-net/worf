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
}
