using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The protocol output plugin writes, as functions for each message.
/// </summary>
internal sealed unsafe class WorfProtoApi
{
    /// <summary>
    /// Gets or sets the function writing a relation, or null when the protocol has none.
    /// </summary>
    public delegate*<StringInfoData*, WorfOutputData, RelationData*, HashSet<int>?, void> WriteRel { get; set; }

    /// <summary>
    /// Gets or sets the function writing BEGIN.
    /// </summary>
    public delegate*<StringInfoData*, WorfOutputData, ReorderBufferTXN*, void> WriteBegin { get; set; }

    /// <summary>
    /// Gets or sets the function writing COMMIT.
    /// </summary>
    public delegate*<StringInfoData*, WorfOutputData, ReorderBufferTXN*, ulong, void> WriteCommit { get; set; }

    /// <summary>
    /// Gets or sets the function writing ORIGIN, or null when the protocol has none.
    /// </summary>
    public delegate*<StringInfoData*, sbyte*, ulong, void> WriteOrigin { get; set; }

    /// <summary>
    /// Gets or sets the function writing INSERT.
    /// </summary>
    public delegate*<StringInfoData*, WorfOutputData, RelationData*, HeapTupleData*, HashSet<int>?, void> WriteInsert { get; set; }

    /// <summary>
    /// Gets or sets the function writing UPDATE.
    /// </summary>
    public delegate*<StringInfoData*, WorfOutputData, RelationData*, HeapTupleData*, HeapTupleData*, HashSet<int>?, void> WriteUpdate { get; set; }

    /// <summary>
    /// Gets or sets the function writing DELETE.
    /// </summary>
    public delegate*<StringInfoData*, WorfOutputData, RelationData*, HeapTupleData*, HashSet<int>?, void> WriteDelete { get; set; }

    /// <summary>
    /// Gets or sets the function writing the startup message.
    /// </summary>
    public delegate*<StringInfoData*, List<(string Key, string Value)>, void> WriteStartupMessage { get; set; }
}
