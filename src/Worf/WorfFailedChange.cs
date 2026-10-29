namespace Worf;

/// <summary>
/// A remote change that failed to apply, and the transaction it belongs to, as <see cref="ExceptionLog"/> records it.
/// </summary>
internal sealed class WorfFailedChange
{
    /// <summary>
    /// Gets the subscription that applied it.
    /// </summary>
    public required WorfSubscription Subscription { get; init; }

    /// <summary>
    /// Gets the node that wrote the transaction.
    /// </summary>
    public uint WriterNodeId { get; init; }

    /// <summary>
    /// Gets the transaction's id on the provider.
    /// </summary>
    public uint RemoteXid { get; init; }

    /// <summary>
    /// Gets when the transaction committed.
    /// </summary>
    public long CommitTime { get; init; }

    /// <summary>
    /// Gets the transaction's commit position on the provider.
    /// </summary>
    public ulong CommitLsn { get; init; }

    /// <summary>
    /// Gets the change's position in the transaction.
    /// </summary>
    public uint ActionCounter { get; init; }

    /// <summary>
    /// Gets what the change does, such as <c>INSERT</c> or <c>QUEUED_SQL</c>, or null.
    /// </summary>
    public string? Operation { get; init; }

    /// <summary>
    /// Gets the table the change writes, or null.
    /// </summary>
    public WorfRelation? Relation { get; init; }

    /// <summary>
    /// Gets the INSERT, UPDATE or DELETE message that carries the change, or null.
    /// </summary>
    public byte[]? Message { get; init; }

    /// <summary>
    /// Gets the queued message, such as DDL, that the change carries, or null.
    /// </summary>
    public string? QueuedMessage { get; init; }
}
