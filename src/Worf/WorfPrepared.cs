namespace Worf;

/// <summary>
/// A transaction prepared after it created worf, changed its nodes or subscriptions, or changed a database, whose end
/// wakes the managers that may then have something to manage.
/// </summary>
internal struct WorfPrepared
{
    /// <summary>
    /// The database the transaction was prepared in, or zero for a free entry.
    /// </summary>
    public uint Dboid;

    /// <summary>
    /// Its transaction ID.
    /// </summary>
    public uint Xid;

    /// <summary>
    /// Whether it may have changed a database, which may give every database's manager something to manage.
    /// </summary>
    public bool EveryDatabase;

    /// <summary>
    /// The databases' state as a manager found the transaction, as <see cref="Worker.DatabasesState"/> gives it, which
    /// wakes every database's manager only if the transaction's end changed it, or zero to wake them whatever it did.
    /// </summary>
    public long Databases;
}
