namespace Worf;

/// <summary>
/// The local node and its interface.
/// </summary>
/// <param name="node">The node.</param>
/// <param name="nodeIf">The node's interface.</param>
internal sealed class WorfLocalNode(WorfNode node, WorfInterface nodeIf)
{
    /// <summary>
    /// Gets the node.
    /// </summary>
    public WorfNode Node { get; } = node;

    /// <summary>
    /// Gets the node's interface.
    /// </summary>
    public WorfInterface NodeIf { get; } = nodeIf;
}
