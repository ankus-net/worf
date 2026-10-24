namespace Worf;

/// <summary>
/// How a conflict was resolved.
/// </summary>
internal enum WorfConflictResolution
{
    /// <summary>
    /// The remote change was applied.
    /// </summary>
    ApplyRemote,

    /// <summary>
    /// The local row was kept.
    /// </summary>
    KeepLocal,

    /// <summary>
    /// The change was skipped.
    /// </summary>
    Skip,
}
