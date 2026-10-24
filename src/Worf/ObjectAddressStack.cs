using Ankus.Postgres;

namespace Worf;

/// <summary>
/// A threaded list of ObjectAddresses, for recursion detection.
/// </summary>
/// <param name="obj">Object being visited.</param>
/// <param name="flags">Its current flag bits.</param>
/// <param name="next">Next outer stack level.</param>
internal sealed class ObjectAddressStack(ObjectAddress obj, int flags, ObjectAddressStack? next)
{
    /// <summary>
    /// Gets the object being visited.
    /// </summary>
    public ObjectAddress Object { get; } = obj;

    /// <summary>
    /// Gets or sets its current flag bits.
    /// </summary>
    public int Flags { get; set; } = flags;

    /// <summary>
    /// Gets the next outer stack level.
    /// </summary>
    public ObjectAddressStack? Next { get; } = next;
}
