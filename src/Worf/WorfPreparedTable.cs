namespace Worf;

/// <summary>
/// The header of the shared table of prepared transactions whose COMMIT PREPARED wakes managers.
/// </summary>
internal struct WorfPreparedTable
{
    /// <summary>
    /// The number of entries in use.
    /// </summary>
    public int Used;
}
