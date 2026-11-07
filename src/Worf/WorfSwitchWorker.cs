namespace Worf;

/// <summary>
/// A switch worker's shared state.
/// </summary>
internal struct WorfSwitchWorker
{
    /// <summary>
    /// The sequence it switches.
    /// </summary>
    public uint Seqoid;

    /// <summary>
    /// The active block the sequence moved backwards in, which it switches from wherever the sequence is in it, or
    /// empty.
    /// </summary>
    public Guid MovedIn;
}
