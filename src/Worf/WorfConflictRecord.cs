namespace Worf;

/// <summary>
/// A conflict that an apply transaction recorded, which is written to the conflict history once it commits. It holds
/// plain values, since the transaction's memory goes as it commits.
/// </summary>
internal sealed class WorfConflictRecord
{
    /// <summary>
    /// Gets what the conflict was, for a warning when it can't be written.
    /// </summary>
    public required string Description { get; init; }

    /// <summary>
    /// Gets when the conflict was recorded, which the partition it goes to covers.
    /// </summary>
    public required PgTimestampTz RecordedAt { get; init; }

    /// <summary>
    /// Gets the subscription.
    /// </summary>
    public required uint SubId { get; init; }

    /// <summary>
    /// Gets the subscription's name.
    /// </summary>
    public required string SubName { get; init; }

    /// <summary>
    /// Gets the kind of conflict.
    /// </summary>
    public required string ConflictType { get; init; }

    /// <summary>
    /// Gets how it was resolved.
    /// </summary>
    public required string Resolution { get; init; }

    /// <summary>
    /// Gets the table's schema.
    /// </summary>
    public required string Schema { get; init; }

    /// <summary>
    /// Gets the table.
    /// </summary>
    public required string Table { get; init; }

    /// <summary>
    /// Gets the index the conflict was found through, if any.
    /// </summary>
    public string? IndexName { get; init; }

    /// <summary>
    /// Gets the local row as JSON, if it is kept.
    /// </summary>
    public string? LocalTuple { get; init; }

    /// <summary>
    /// Gets the transaction that wrote the local row, as text, if known.
    /// </summary>
    public string? LocalXid { get; init; }

    /// <summary>
    /// Gets the local row's replication origin, if known.
    /// </summary>
    public int? LocalOrigin { get; init; }

    /// <summary>
    /// Gets the local row's commit time, if known.
    /// </summary>
    public PgTimestampTz? LocalCommitTs { get; init; }

    /// <summary>
    /// Gets the remote row as JSON, if it is kept.
    /// </summary>
    public string? RemoteTuple { get; init; }

    /// <summary>
    /// Gets the remote transaction's replication origin.
    /// </summary>
    public required int RemoteOrigin { get; init; }

    /// <summary>
    /// Gets the remote transaction's commit time.
    /// </summary>
    public required PgTimestampTz RemoteCommitTs { get; init; }

    /// <summary>
    /// Gets the remote transaction's commit position.
    /// </summary>
    public required ulong RemoteCommitLsn { get; init; }

    /// <summary>
    /// Gets whether the table has row triggers that run before a change.
    /// </summary>
    public required bool HasBeforeTriggers { get; init; }
}
