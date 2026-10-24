namespace Worf;

/// <summary>
/// A message read from the queue table.
/// </summary>
internal sealed class QueuedMessage
{
    /// <summary>
    /// Gets or sets when the message was queued.
    /// </summary>
    public long QueuedAt { get; set; }

    /// <summary>
    /// Gets or sets the replication sets the message replicates through, or null for all.
    /// </summary>
    public List<string>? ReplicationSets { get; set; }

    /// <summary>
    /// Gets or sets the role that queued the message.
    /// </summary>
    public string Role { get; set; } = "";

    /// <summary>
    /// Gets or sets the message type.
    /// </summary>
    public char MessageType { get; set; }

    /// <summary>
    /// Gets or sets the message's JSON text.
    /// </summary>
    public string Message { get; set; } = "";
}
