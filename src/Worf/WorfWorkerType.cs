namespace Worf;

/// <summary>
/// The kinds of worf worker.
/// </summary>
internal enum WorfWorkerType
{
    /// <summary>
    /// Unused slot.
    /// </summary>
    None,

    /// <summary>
    /// Manager.
    /// </summary>
    Manager,

    /// <summary>
    /// Apply.
    /// </summary>
    Apply,

    /// <summary>
    /// Special type of Apply that synchronizes one table.
    /// </summary>
    Sync,
}
