namespace Worf;

/// <summary>
/// A subscription.
/// </summary>
internal sealed class WorfSubscription
{
    /// <summary>
    /// Gets or sets the subscription's id.
    /// </summary>
    public uint Id { get; set; }

    /// <summary>
    /// Gets or sets the subscription's name.
    /// </summary>
    public string Name { get; set; } = "";

    /// <summary>
    /// Gets or sets the provider node.
    /// </summary>
    public WorfNode Origin { get; set; } = new();

    /// <summary>
    /// Gets or sets the subscriber node.
    /// </summary>
    public WorfNode Target { get; set; } = new();

    /// <summary>
    /// Gets or sets the provider's interface.
    /// </summary>
    public WorfInterface OriginIf { get; set; } = new();

    /// <summary>
    /// Gets or sets the subscriber's interface.
    /// </summary>
    public WorfInterface TargetIf { get; set; } = new();

    /// <summary>
    /// Gets or sets whether the subscription is enabled.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Gets or sets the apply delay, or null.
    /// </summary>
    public PgInterval? ApplyDelay { get; set; }

    /// <summary>
    /// Gets or sets the slot name.
    /// </summary>
    public string SlotName { get; set; } = "";

    /// <summary>
    /// Gets or sets the subscribed replication sets.
    /// </summary>
    public List<string> ReplicationSets { get; set; } = [];

    /// <summary>
    /// Gets or sets the forwarded origins.
    /// </summary>
    public List<string> ForwardOrigins { get; set; } = [];

    /// <summary>
    /// Gets or sets whether to transfer data as text.
    /// </summary>
    public bool ForceTextTransfer { get; set; }
}
