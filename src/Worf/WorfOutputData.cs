namespace Worf;

/// <summary>
/// The output plugin's state for one decoding context.
/// </summary>
internal sealed class WorfOutputData
{
    /// <summary>
    /// Gets or sets the protocol the plugin writes.
    /// </summary>
    public WorfProtoApi? Api { get; set; }

    /// <summary>
    /// Gets or sets the cached node id.
    /// </summary>
    public uint LocalNodeId { get; set; }

    /// <summary>
    /// Gets or sets whether internal binary representations may be sent.
    /// </summary>
    public bool AllowInternalBasetypes { get; set; }

    /// <summary>
    /// Gets or sets whether send/recv representations may be sent.
    /// </summary>
    public bool AllowBinaryBasetypes { get; set; }

    /// <summary>
    /// Gets or sets whether changeset origins are forwarded.
    /// </summary>
    public bool ForwardChangesetOrigins { get; set; }

    /// <summary>
    /// Gets or sets the encoding datums are sent in.
    /// </summary>
    public int FieldDatumEncoding { get; set; }

    /// <summary>
    /// Gets or sets the client's PostgreSQL version.
    /// </summary>
    public uint ClientPgVersion { get; set; }

    /// <summary>
    /// Gets or sets the greatest protocol version the client supports.
    /// </summary>
    public uint ClientMaxProtoVersion { get; set; }

    /// <summary>
    /// Gets or sets the least protocol version the client supports.
    /// </summary>
    public uint ClientMinProtoVersion { get; set; }

    /// <summary>
    /// Gets or sets the protocol version the plugin writes: the newest both it and the client support.
    /// </summary>
    public uint ProtoVersion { get; set; }

    /// <summary>
    /// Gets or sets the encoding the client expects.
    /// </summary>
    public string? ClientExpectedEncoding { get; set; }

    /// <summary>
    /// Gets or sets the protocol format the client asked for.
    /// </summary>
    public string? ClientProtocolFormat { get; set; }

    /// <summary>
    /// Gets or sets the major version of the client's binary base types.
    /// </summary>
    public uint ClientBinaryBasetypesMajorVersion { get; set; }

    /// <summary>
    /// Gets or sets whether the client said whether it wants internal base types.
    /// </summary>
    public bool ClientWantInternalBasetypesSet { get; set; }

    /// <summary>
    /// Gets or sets whether the client wants internal base types.
    /// </summary>
    public bool ClientWantInternalBasetypes { get; set; }

    /// <summary>
    /// Gets or sets whether the client said whether it wants binary base types.
    /// </summary>
    public bool ClientWantBinaryBasetypesSet { get; set; }

    /// <summary>
    /// Gets or sets whether the client wants binary base types.
    /// </summary>
    public bool ClientWantBinaryBasetypes { get; set; }

    /// <summary>
    /// Gets or sets whether the client sent its endianness.
    /// </summary>
    public bool ClientBinaryBigendianSet { get; set; }

    /// <summary>
    /// Gets or sets whether the client is big-endian.
    /// </summary>
    public bool ClientBinaryBigendian { get; set; }

    /// <summary>
    /// Gets or sets the client's <c>sizeof(Datum)</c>.
    /// </summary>
    public uint ClientBinarySizeofdatum { get; set; }

    /// <summary>
    /// Gets or sets the client's <c>sizeof(int)</c>.
    /// </summary>
    public uint ClientBinarySizeofint { get; set; }

    /// <summary>
    /// Gets or sets the client's <c>sizeof(long)</c>.
    /// </summary>
    public uint ClientBinarySizeoflong { get; set; }

    /// <summary>
    /// Gets or sets whether the client sent whether float4 is passed by value.
    /// </summary>
    public bool ClientBinaryFloat4byvalSet { get; set; }

    /// <summary>
    /// Gets or sets whether the client passes float4 by value.
    /// </summary>
    public bool ClientBinaryFloat4byval { get; set; }

    /// <summary>
    /// Gets or sets whether the client sent whether float8 is passed by value.
    /// </summary>
    public bool ClientBinaryFloat8byvalSet { get; set; }

    /// <summary>
    /// Gets or sets whether the client passes float8 by value.
    /// </summary>
    public bool ClientBinaryFloat8byval { get; set; }

    /// <summary>
    /// Gets or sets whether the client sent whether it uses integer datetimes.
    /// </summary>
    public bool ClientBinaryIntdatetimesSet { get; set; }

    /// <summary>
    /// Gets or sets whether the client uses integer datetimes.
    /// </summary>
    public bool ClientBinaryIntdatetimes { get; set; }

    /// <summary>
    /// Gets or sets whether the client wants transaction info left out.
    /// </summary>
    public bool ClientNoTxinfo { get; set; }

    /// <summary>
    /// Gets or sets the forwarded origin names.
    /// </summary>
    public List<string> ForwardOrigins { get; set; } = [];

    /// <summary>
    /// Gets or sets the replication sets.
    /// </summary>
    public List<WorfRepSet> ReplicationSets { get; set; } = [];

    /// <summary>
    /// Gets or sets the only table to replicate, as schema and name, or null.
    /// </summary>
    public (string Schemaname, string Relname)? ReplicateOnlyTable { get; set; }
}
