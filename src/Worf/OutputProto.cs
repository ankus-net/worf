namespace Worf;

/// <summary>
/// worf protocol functions.
/// </summary>
internal static unsafe class OutputProto
{
    /// <summary>
    /// Our native protocol and the greatest version we can support.
    /// </summary>
    internal const int ProtoVersionNum = 2;

    /// <summary>
    /// The oldest protocol version we have backwards compatibility for.
    /// </summary>
    internal const int ProtoMinVersionNum = 1;

    /// <summary>
    /// The protocol version from which a RELATION message carries each column's type.
    /// </summary>
    internal const int ColumnTypesVersionNum = 2;

    /// <summary>
    /// The startup parameter format, versioned separately to the rest of the wire protocol because we negotiate the
    /// wire protocol version using the startup parameters sent to us.
    /// </summary>
    internal const int StartupParamFormatFlat = 1;

    /// <summary>
    /// The startup reply message format, versioned separately to the rest of the protocol.
    /// </summary>
    internal const byte StartupMsgFormatFlat = 1;

    /// <summary>
    /// Selects a protocol's functions.
    /// </summary>
    /// <param name="typ">The protocol.</param>
    /// <returns>The functions.</returns>
    internal static WorfProtoApi InitApi(WorfProtoType typ)
    {
        WorfProtoApi res = new();

        if (typ == WorfProtoType.Json)
        {
            res.WriteRel = null;
            res.WriteBegin = &ProtoJson.WriteBegin;
            res.WriteCommit = &ProtoJson.WriteCommit;
            res.WriteOrigin = null;
            res.WriteInsert = &ProtoJson.WriteInsert;
            res.WriteUpdate = &ProtoJson.WriteUpdate;
            res.WriteDelete = &ProtoJson.WriteDelete;
            res.WriteStartupMessage = &ProtoJson.WriteStartupMessage;
        }
        else
        {
            res.WriteRel = &ProtoNative.WriteRel;
            res.WriteBegin = &ProtoNative.WriteBegin;
            res.WriteCommit = &ProtoNative.WriteCommit;
            res.WriteOrigin = &ProtoNative.WriteOrigin;
            res.WriteInsert = &ProtoNative.WriteInsert;
            res.WriteUpdate = &ProtoNative.WriteUpdate;
            res.WriteDelete = &ProtoNative.WriteDelete;
            res.WriteStartupMessage = &ProtoNative.WriteStartupMessage;
        }

        return res;
    }
}
