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

    /// <summary>
    /// Keeps a database's managed sequences supplied with blocks.
    /// </summary>
    Range,

    /// <summary>
    /// Exchanges a database's sequence ranges with its neighbors, for its range worker.
    /// </summary>
    Exchange,
}
