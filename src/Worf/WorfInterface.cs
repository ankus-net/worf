namespace Worf;

/// <summary>
/// A node's connection interface.
/// </summary>
internal sealed class WorfInterface
{
    /// <summary>
    /// Gets or sets the interface's id.
    /// </summary>
    public uint Id { get; set; }

    /// <summary>
    /// Gets or sets the interface's name.
    /// </summary>
    public string Name { get; set; } = "";

    /// <summary>
    /// Gets or sets the node the interface belongs to.
    /// </summary>
    public uint NodeId { get; set; }

    /// <summary>
    /// Gets or sets the interface's connection string.
    /// </summary>
    public string Dsn { get; set; } = "";
}
