using System.Globalization;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Logical Replication output plugin configuration handling.
/// </summary>
internal static class OutputConfig
{
    /// <summary>
    /// Gets whether <c>USE_FLOAT4_BYVAL</c> is defined.
    /// </summary>
    internal static bool ServerFloat4Byval => Worf.Float4Byval;

    /// <summary>
    /// Gets whether <c>USE_FLOAT8_BYVAL</c> is defined.
    /// </summary>
    internal static bool ServerFloat8Byval => Worf.Float8Byval;

    /// <summary>
    /// Gets whether <c>USE_INTEGER_DATETIMES</c> is defined.
    /// </summary>
    internal static bool ServerIntegerDatetimes => Worf.IntegerDatetimes;

    /// <summary>
    /// Gets whether <c>WORDS_BIGENDIAN</c> is defined.
    /// </summary>
    internal static bool ServerBigendian => Worf.BigEndian;

    /// <summary>
    /// Read parameters sent by client at startup and store recognised ones in the output data.
    /// </summary>
    /// <param name="options">The client's options.</param>
    /// <param name="data">The output data, with every client-supplied field unset.</param>
    /// <returns>The parameter format.</returns>
    internal static int ProcessParameters(List<(string Name, string? Value)> options, WorfOutputData data)
    {
        int paramsFormat = (int)GetParamUint32(options, "startup_params_format");

        if (paramsFormat == OutputProto.StartupParamFormatFlat)
        {
            ProcessParametersV1(options, data);
        }
        else
        {
            throw new PgException(PgSqlStates.FeatureNotSupported,
                $"startup_params_format {paramsFormat} not supported, only version {OutputProto.StartupParamFormatFlat} supported");
        }

        return paramsFormat;
    }

    /// <summary>
    /// This builds the protocol startup message, which is always the first message on the wire after the client
    /// sends START_REPLICATION. It confirms to the client that we could apply requested options, and tells the
    /// client our capabilities.
    /// </summary>
    /// <param name="data">The output data.</param>
    /// <returns>The key and value pairs.</returns>
    internal static List<(string Key, string Value)> PrepareStartupMessage(WorfOutputData data)
    {
        int pgVersionNum = Worf.ServerVersionNum;
        List<(string Key, string Value)> l =
        [
            ("max_proto_version", Int(OutputProto.ProtoVersionNum)),
            ("min_proto_version", Int(OutputProto.ProtoMinVersionNum)),

            // Whether RELATION messages carry the columns' types
            ("coltypes", Bool(data.ProtoVersion >= OutputProto.ColumnTypesVersionNum)),

            // Info about our Pg host
            ("pg_version_num", Int(pgVersionNum)),
            ("pg_version", Worf.GetConfigOptionByName("server_version")!),
            ("pg_catversion", Int(CatalogVersionNo)),
            ("database_encoding", Worf.DatabaseEncodingName),
            ("encoding", EncodingToChar(data.FieldDatumEncoding)),
            ("forward_changeset_origins", Bool(data.ForwardChangesetOrigins)),
            ("walsender_pid", Int(Environment.ProcessId)),

            // and ourselves
            ("worf_version", Worf.Version),
            ("worf_version_num", Int(Worf.VersionNum)),

            // binary options enabled
            ("binary.internal_basetypes", Bool(data.AllowInternalBasetypes)),
            ("binary.binary_basetypes", Bool(data.AllowBinaryBasetypes)),

            // Binary format characteristics of server
            ("binary.basetypes_major_version", Int(pgVersionNum / 100)),
            ("binary.sizeof_int", Int(Worf.SizeofInt)),
            ("binary.sizeof_long", Int(Worf.SizeofLong)),
            ("binary.sizeof_datum", Int(Worf.SizeofDatum)),
            ("binary.maxalign", Int(MaximumAlignof)),
            ("binary.bigendian", Bool(ServerBigendian)),
            ("binary.float4_byval", Bool(ServerFloat4Byval)),
            ("binary.float8_byval", Bool(ServerFloat8Byval)),
            ("binary.integer_datetimes", Bool(ServerIntegerDatetimes)),

            // We don't know how to send in anything except our host's format
            ("binary.binary_pg_version", Int(pgVersionNum / 100)),
            ("no_txinfo", Bool(data.ClientNoTxinfo)),
        ];

        return l;
    }

    /// <summary>
    /// Gets the name of an encoding, as <c>pg_encoding_to_char</c> returns it.
    /// </summary>
    /// <param name="encoding">The encoding.</param>
    /// <returns>The name.</returns>
    internal static unsafe string EncodingToChar(int encoding)
    {
#if ANKUS_PG15 || ANKUS_PG16
        return Worf.FromServer(NativeMethods.pg_encoding_to_char(encoding));
#else
        return Worf.FromServer(NativeMethods.pg_encoding_to_char_private(encoding));
#endif
    }

    /// <summary>
    /// Gets an encoding by its name, as <c>pg_char_to_encoding</c> does.
    /// </summary>
    /// <param name="name">The name.</param>
    /// <returns>The encoding, or -1.</returns>
    internal static unsafe int CharToEncoding(string name)
    {
        fixed (byte* cname = Worf.ToServerCString(name))
        {
#if ANKUS_PG15 || ANKUS_PG16
            return NativeMethods.pg_char_to_encoding((sbyte*)cname);
#else
            return NativeMethods.pg_char_to_encoding_private((sbyte*)cname);
#endif
        }
    }

    /// <summary>
    /// Gets <c>CATALOG_VERSION_NO</c> of the running server.
    /// </summary>
    private static int CatalogVersionNo
    {
        get
        {
            unsafe
            {
                bool crcOk;
                ControlFileData* controlFile = NativeMethods.get_controlfile(NativeGlobals.DataDir, &crcOk);
                int catversion = (int)controlFile->catalog_version_no;
                NativeMethods.pfree(controlFile);
                return catversion;
            }
        }
    }

    /// <summary>
    /// Gets <c>MAXIMUM_ALIGNOF</c>, which is 8 on every supported platform.
    /// </summary>
    private static int MaximumAlignof => 8;

    private static OutputPluginParamKey GetParamKey(string paramName) => paramName switch
    {
        "max_proto_version" => OutputPluginParamKey.MaxProtocolVersion,
        "min_proto_version" => OutputPluginParamKey.MinProtocolVersion,
        "proto_format" => OutputPluginParamKey.ProtocolFormat,
        "expected_encoding" => OutputPluginParamKey.ExpectedEncoding,
        "binary.bigendian" => OutputPluginParamKey.BinaryBigendian,
        "binary.sizeof_datum" => OutputPluginParamKey.BinarySizeofDatum,
        "binary.sizeof_int" => OutputPluginParamKey.BinarySizeofInt,
        "binary.sizeof_long" => OutputPluginParamKey.BinarySizeofLong,
        "binary.float4_byval" => OutputPluginParamKey.BinaryFloat4byval,
        "binary.float8_byval" => OutputPluginParamKey.BinaryFloat8byval,
        "binary.integer_datetimes" => OutputPluginParamKey.BinaryIntegerDatetimes,
        "binary.want_internal_basetypes" => OutputPluginParamKey.BinaryWantInternalBasetypes,
        "binary.want_binary_basetypes" => OutputPluginParamKey.BinaryWantBinaryBasetypes,
        "binary.basetypes_major_version" => OutputPluginParamKey.BinaryBasetypesMajorVersion,
        "worf.forward_origins" => OutputPluginParamKey.WorfForwardOrigins,
        "worf.replication_set_names" => OutputPluginParamKey.WorfReplicationSetNames,
        "worf.replicate_only_table" => OutputPluginParamKey.WorfReplicateOnlyTable,
        "hooks.setup_function" => OutputPluginParamKey.HooksSetupFunction,
        "pg_version" => OutputPluginParamKey.PgVersion,
        "no_txinfo" => OutputPluginParamKey.NoTxinfo,
        _ => OutputPluginParamKey.Unrecognised,
    };

    private static void ProcessParametersV1(List<(string Name, string? Value)> options, WorfOutputData data)
    {
        // max_proto_version and min_proto_version are specified as required, and must be parsed before anything
        // else.
        data.ClientMaxProtoVersion = GetParamUint32(options, "max_proto_version");
        data.ClientMinProtoVersion = GetParamUint32(options, "min_proto_version");

        // Examine all the other params in the v1 message.
        foreach ((string name, string? value) elem in options)
        {
            // Check each param, whether or not we recognise it
            switch (GetParamKey(elem.name))
            {
                case OutputPluginParamKey.BinaryBigendian:
                    data.ClientBinaryBigendianSet = true;
                    data.ClientBinaryBigendian = ParseParamBool(elem);
                    break;

                case OutputPluginParamKey.BinarySizeofDatum:
                    data.ClientBinarySizeofdatum = ParseParamUint32(elem);
                    break;

                case OutputPluginParamKey.BinarySizeofInt:
                    data.ClientBinarySizeofint = ParseParamUint32(elem);
                    break;

                case OutputPluginParamKey.BinarySizeofLong:
                    data.ClientBinarySizeoflong = ParseParamUint32(elem);
                    break;

                case OutputPluginParamKey.BinaryFloat4byval:
                    data.ClientBinaryFloat4byvalSet = true;
                    data.ClientBinaryFloat4byval = ParseParamBool(elem);
                    break;

                case OutputPluginParamKey.BinaryFloat8byval:
                    data.ClientBinaryFloat4byvalSet = true;
                    data.ClientBinaryFloat4byval = ParseParamBool(elem);
                    break;

                case OutputPluginParamKey.BinaryIntegerDatetimes:
                    data.ClientBinaryIntdatetimesSet = true;
                    data.ClientBinaryIntdatetimes = ParseParamBool(elem);
                    break;

                case OutputPluginParamKey.ProtocolFormat:
                    data.ClientProtocolFormat = ParamString(elem);
                    break;

                case OutputPluginParamKey.ExpectedEncoding:
                    data.ClientExpectedEncoding = ParamString(elem);
                    break;

                case OutputPluginParamKey.PgVersion:
                    data.ClientPgVersion = ParseParamUint32(elem);
                    break;

                case OutputPluginParamKey.BinaryWantInternalBasetypes:
                    // check if we want to use internal data representation
                    data.ClientWantInternalBasetypesSet = true;
                    data.ClientWantInternalBasetypes = ParseParamBool(elem);
                    break;

                case OutputPluginParamKey.BinaryWantBinaryBasetypes:
                    // check if we want to use binary data representation
                    data.ClientWantBinaryBasetypesSet = true;
                    data.ClientWantBinaryBasetypes = ParseParamBool(elem);
                    break;

                case OutputPluginParamKey.BinaryBasetypesMajorVersion:
                    data.ClientBinaryBasetypesMajorVersion = ParseParamUint32(elem);
                    break;

                case OutputPluginParamKey.WorfForwardOrigins:
                {
                    string val = ParamString(elem);
                    if (!SplitIdentifierString(val, ',', out List<string> forwardOriginNames))
                    {
                        throw new PgException(PgSqlStates.InternalError, $"Could not parse forward origin name list {val}");
                    }

                    foreach (string originName in forwardOriginNames)
                    {
                        if (originName != Worf.ReplicationOriginAll)
                        {
                            throw new PgException(PgSqlStates.InternalError,
                                $"Only \"{Worf.ReplicationOriginAll}\" is allowed in forward origin name list at the moment, found \"{originName}\"");
                        }
                    }

                    data.ForwardOrigins = forwardOriginNames;
                    break;
                }

                case OutputPluginParamKey.WorfReplicationSetNames:
                {
                    string val = ParamString(elem);
                    if (!SplitIdentifierString(val, ',', out List<string> replicationSetNames))
                    {
                        throw new PgException(PgSqlStates.InternalError, $"Could not parse replication set name list {val}");
                    }

                    data.ReplicationSets = ReplicationSet.GetReplicationSets(data.LocalNodeId, replicationSetNames, false);
                    break;
                }

                case OutputPluginParamKey.WorfReplicateOnlyTable:
                {
                    string val = ParamString(elem);
                    if (!SplitIdentifierString(val, '.', out List<string> replicateOnlyTable))
                    {
                        throw new PgException(PgSqlStates.InternalError, $"Could not parse replicate_only_table {val}");
                    }

                    data.ReplicateOnlyTable = (replicateOnlyTable[0], replicateOnlyTable[1]);
                    break;
                }

                case OutputPluginParamKey.NoTxinfo:
                    data.ClientNoTxinfo = ParseParamBool(elem);
                    break;

                // Backwards compat.
                case OutputPluginParamKey.HooksSetupFunction:
                    break;

                case OutputPluginParamKey.Unrecognised:
                    PgLog.Debug1($"Unrecognised worf parameter {elem.name} ignored");
                    break;

                default:
                    break;
            }
        }
    }

    private static string ParamString((string Name, string? Value) elem)
        => elem.Value ?? throw new PgException(PgSqlStates.InvalidParameterValue, $"parameter \"{elem.Name}\" cannot be NULL");

    private static uint GetParamUint32(List<(string Name, string? Value)> options, string name)
    {
        foreach ((string Name, string? Value) elem in options)
        {
            // Search until matching parameter found
            if (!string.Equals(name, elem.Name, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            return ParseParamUint32(elem);
        }

        throw new PgException(PgSqlStates.InvalidParameterValue, $"missing required parameter \"{name}\"");
    }

    private static unsafe bool ParseParamBool((string Name, string? Value) elem)
    {
        string value = ParamString(elem);
        bool res;
        fixed (byte* cvalue = Worf.ToServerCString(value))
        {
            if (!NativeMethods.parse_bool((sbyte*)cvalue, &res))
            {
                throw new PgException(PgSqlStates.InvalidParameterValue,
                    $"could not parse boolean value \"{value}\" for parameter \"{elem.Name}\"");
            }
        }

        return res;
    }

    private static uint ParseParamUint32((string Name, string? Value) elem)
    {
        string value = ParamString(elem);
        long res = StrToLl(value, out bool overflow);
        if (overflow)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue,
                $"could not parse integer value \"{value}\" for parameter \"{elem.Name}\"");
        }

        if (res > uint.MaxValue || res < 0)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue,
                $"value \"{value}\" out of range for parameter \"{elem.Name}\"");
        }

        return (uint)res;
    }

    /// <summary>
    /// Parses a base 10 integer as <c>strtoll</c> does, reporting <c>ERANGE</c> as overflow.
    /// </summary>
    private static long StrToLl(string value, out bool overflow)
    {
        overflow = false;
        int i = 0;
        while (i < value.Length && char.IsWhiteSpace(value[i]))
        {
            i++;
        }

        int start = i;
        if (i < value.Length && (value[i] == '+' || value[i] == '-'))
        {
            i++;
        }

        int digits = i;
        while (i < value.Length && char.IsAsciiDigit(value[i]))
        {
            i++;
        }

        if (i == digits)
        {
            return 0;
        }

        if (!long.TryParse(value.AsSpan(start, i - start), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long res))
        {
            overflow = true;
            return 0;
        }

        return res;
    }

    /// <summary>
    /// Splits a list of identifiers, as <c>SplitIdentifierString</c> does.
    /// </summary>
    private static unsafe bool SplitIdentifierString(string rawstring, char separator, out List<string> namelist)
    {
        namelist = [];
        List* list = null;
        if (!NativeMethods.SplitIdentifierString(Worf.PStrDup(rawstring), (sbyte)separator, &list))
        {
            return false;
        }

        for (int i = 0; list is not null && i < list->length; i++)
        {
            namelist.Add(Worf.FromServer((sbyte*)list->elements[i].ptr_value));
        }

        return true;
    }

    private static string Bool(bool val) => val ? "t" : "f";

    private static string Int(int val) => val.ToString(CultureInfo.InvariantCulture);
}
