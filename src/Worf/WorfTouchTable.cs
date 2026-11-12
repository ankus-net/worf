namespace Worf;

/// <summary>
/// The header of the shared counters of changes to each managed sequence's ranges.
/// </summary>
internal struct WorfTouchTable
{
    /// <summary>
    /// How often a change that names no sequence happened, which counts for every sequence.
    /// </summary>
    public long All;
}
