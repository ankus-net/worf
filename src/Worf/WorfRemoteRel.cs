namespace Worf;

/// <summary>
/// A relation as the remote side describes it.
/// </summary>
internal sealed class WorfRemoteRel
{
    /// <summary>
    /// Gets or sets the remote relation's id.
    /// </summary>
    public uint Relid { get; set; }

    /// <summary>
    /// Gets or sets the relation's schema.
    /// </summary>
    public string Nspname { get; set; } = "";

    /// <summary>
    /// Gets or sets the relation's name.
    /// </summary>
    public string Relname { get; set; } = "";

    /// <summary>
    /// Gets or sets the replicated columns' names.
    /// </summary>
    public List<string> Attnames { get; set; } = [];

    /// <summary>
    /// Gets or sets whether the table has a row filter. Only returned by info function, not protocol.
    /// </summary>
    public bool HasRowFilter { get; set; }
}
