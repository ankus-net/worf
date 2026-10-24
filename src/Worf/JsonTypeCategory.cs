namespace Worf;

/// <summary>
/// Type categories for <c>datum_to_json</c>.
/// </summary>
internal enum JsonTypeCategory
{
    /// <summary>
    /// Null, so we didn't bother to identify.
    /// </summary>
    Null,

    /// <summary>
    /// Boolean (built-in types only).
    /// </summary>
    Bool,

    /// <summary>
    /// Numeric (ditto).
    /// </summary>
    Numeric,

    /// <summary>
    /// A date, which uses special datetime formatting.
    /// </summary>
    Date,

    /// <summary>
    /// A timestamp, formatted as a datetime.
    /// </summary>
    Timestamp,

    /// <summary>
    /// A timestamp with time zone, formatted as a datetime.
    /// </summary>
    TimestampTz,

    /// <summary>
    /// JSON itself (and JSONB).
    /// </summary>
    Json,

    /// <summary>
    /// Array.
    /// </summary>
    Array,

    /// <summary>
    /// Composite.
    /// </summary>
    Composite,

    /// <summary>
    /// Something with an explicit cast to JSON.
    /// </summary>
    Cast,

    /// <summary>
    /// All else.
    /// </summary>
    Other,
}
