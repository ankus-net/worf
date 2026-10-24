using Ankus.Postgres;

namespace Worf;

/// <summary>
/// An expansible list of ObjectAddresses.
/// </summary>
internal sealed class ObjectAddresses
{
    /// <summary>
    /// Gets the addresses.
    /// </summary>
    public List<ObjectAddress> Refs { get; } = [];

    /// <summary>
    /// Gets the addresses' extra data.
    /// </summary>
    public List<ObjectAddressExtra> Extras { get; } = [];
}
