namespace Worf;

/// <summary>
/// A block a session asked for, which the database's range worker records as a request, since the session can't
/// commit in the middle of its transaction.
/// </summary>
internal struct WorfReservation
{
    /// <summary>
    /// The request, or the empty id for an unused entry.
    /// </summary>
    public Guid RequestId;

    /// <summary>
    /// The sequence's database.
    /// </summary>
    public uint Dboid;

    /// <summary>
    /// The sequence.
    /// </summary>
    public uint Seqoid;

    /// <summary>
    /// The kind of block: 'n' for a node block of at least <see cref="Size"/> values, 'i' for exactly
    /// [<see cref="Lo"/>, <see cref="Hi"/>].
    /// </summary>
    public byte Kind;

    /// <summary>
    /// The block's size, in steps of the increment.
    /// </summary>
    public long Size;

    /// <summary>
    /// The start of an imported block.
    /// </summary>
    public long Lo;

    /// <summary>
    /// The end of an imported block.
    /// </summary>
    public long Hi;
}
