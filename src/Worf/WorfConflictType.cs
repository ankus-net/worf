namespace Worf;

/// <summary>
/// The kinds of conflict the apply worker detects.
/// </summary>
internal enum WorfConflictType
{
    /// <summary>
    /// An insert found an existing row.
    /// </summary>
    InsertInsert,

    /// <summary>
    /// An update found a row another origin changed.
    /// </summary>
    UpdateUpdate,

    /// <summary>
    /// An update found no row.
    /// </summary>
    UpdateDelete,

    /// <summary>
    /// A delete found no row.
    /// </summary>
    DeleteDelete,
}
