namespace Worf;

/// <summary>
/// A managed sequence's count of changes to its ranges, in shared memory, by its database and its OID: an entry with
/// neither is free, and one of a sequence that was dropped, with no database and the largest OID, may be taken again.
/// </summary>
internal struct WorfTouch
{
    /// <summary>
    /// The sequence's database.
    /// </summary>
    public uint Dboid;

    /// <summary>
    /// The sequence.
    /// </summary>
    public uint Seqoid;

    /// <summary>
    /// The count of its changes.
    /// </summary>
    public long Count;
}
