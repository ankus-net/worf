namespace Worf;

/// <summary>
/// A node, and the functions that manipulate the node and subscription catalogs.
/// </summary>
internal sealed partial class WorfNode
{
    /// <summary>
    /// Gets or sets the node's id.
    /// </summary>
    public uint Id { get; set; }

    /// <summary>
    /// Gets or sets the node's name.
    /// </summary>
    public string Name { get; set; } = "";
}
