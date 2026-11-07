namespace Worf;

/// <summary>
/// A transaction prepared after it created worf, changed its nodes or subscriptions, or changed a database, whose
/// COMMIT PREPARED wakes the managers that may then have something to manage.
/// </summary>
internal struct WorfPrepared
{
    /// <summary>
    /// The database the transaction was prepared in, or zero for a free entry.
    /// </summary>
    public uint Dboid;

    /// <summary>
    /// The hash of its name.
    /// </summary>
    public uint GidHash;

    /// <summary>
    /// Whether it changed a database, which may give every database's manager something to manage.
    /// </summary>
    public bool EveryDatabase;

    /// <summary>
    /// When it was prepared.
    /// </summary>
    public long PreparedAt;
}
