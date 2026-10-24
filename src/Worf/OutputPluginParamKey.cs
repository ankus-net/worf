namespace Worf;

/// <summary>
/// The startup parameters the output plugin reads.
/// </summary>
internal enum OutputPluginParamKey
{
    /// <summary>
    /// A parameter the plugin doesn't recognise.
    /// </summary>
    Unrecognised,

    /// <summary>
    /// The <c>max_proto_version</c> parameter.
    /// </summary>
    MaxProtocolVersion,

    /// <summary>
    /// The <c>min_proto_version</c> parameter.
    /// </summary>
    MinProtocolVersion,

    /// <summary>
    /// The <c>proto_format</c> parameter.
    /// </summary>
    ProtocolFormat,

    /// <summary>
    /// The <c>expected_encoding</c> parameter.
    /// </summary>
    ExpectedEncoding,

    /// <summary>
    /// The <c>binary.bigendian</c> parameter.
    /// </summary>
    BinaryBigendian,

    /// <summary>
    /// The <c>binary.sizeof_datum</c> parameter.
    /// </summary>
    BinarySizeofDatum,

    /// <summary>
    /// The <c>binary.sizeof_int</c> parameter.
    /// </summary>
    BinarySizeofInt,

    /// <summary>
    /// The <c>binary.sizeof_long</c> parameter.
    /// </summary>
    BinarySizeofLong,

    /// <summary>
    /// The <c>binary.float4_byval</c> parameter.
    /// </summary>
    BinaryFloat4byval,

    /// <summary>
    /// The <c>binary.float8_byval</c> parameter.
    /// </summary>
    BinaryFloat8byval,

    /// <summary>
    /// The <c>binary.integer_datetimes</c> parameter.
    /// </summary>
    BinaryIntegerDatetimes,

    /// <summary>
    /// The <c>binary.want_internal_basetypes</c> parameter.
    /// </summary>
    BinaryWantInternalBasetypes,

    /// <summary>
    /// The <c>binary.want_binary_basetypes</c> parameter.
    /// </summary>
    BinaryWantBinaryBasetypes,

    /// <summary>
    /// The <c>binary.basetypes_major_version</c> parameter.
    /// </summary>
    BinaryBasetypesMajorVersion,

    /// <summary>
    /// The <c>worf.forward_origins</c> parameter.
    /// </summary>
    WorfForwardOrigins,

    /// <summary>
    /// The <c>worf.replication_set_names</c> parameter.
    /// </summary>
    WorfReplicationSetNames,

    /// <summary>
    /// The <c>worf.replicate_only_table</c> parameter.
    /// </summary>
    WorfReplicateOnlyTable,

    /// <summary>
    /// The <c>hooks.setup_function</c> parameter.
    /// </summary>
    HooksSetupFunction,

    /// <summary>
    /// The <c>pg_version</c> parameter.
    /// </summary>
    PgVersion,

    /// <summary>
    /// The <c>no_txinfo</c> parameter.
    /// </summary>
    NoTxinfo,
}
