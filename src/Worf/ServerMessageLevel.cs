namespace Worf;

/// <summary>
/// Labels the server message levels that <c>worf.conflict_log_level</c> accepts, copied from guc.c.
/// </summary>
internal enum ServerMessageLevel
{
    /// <summary>
    /// The hidden alias of <c>debug2</c>, which is shown for that level.
    /// </summary>
    [PgGucLabel("debug", Hidden = true)]
    Debug = 13,

    /// <summary>
    /// The <c>debug5</c> level.
    /// </summary>
    [PgGucLabel("debug5")]
    Debug5 = 10,

    /// <summary>
    /// The <c>debug4</c> level.
    /// </summary>
    [PgGucLabel("debug4")]
    Debug4 = 11,

    /// <summary>
    /// The <c>debug3</c> level.
    /// </summary>
    [PgGucLabel("debug3")]
    Debug3 = 12,

    /// <summary>
    /// The <c>debug2</c> level.
    /// </summary>
    [PgGucLabel("debug2")]
    Debug2 = Debug,

    /// <summary>
    /// The <c>debug1</c> level.
    /// </summary>
    [PgGucLabel("debug1")]
    Debug1 = 14,

    /// <summary>
    /// The <c>info</c> level.
    /// </summary>
    [PgGucLabel("info")]
    Info = 17,

    /// <summary>
    /// The <c>notice</c> level.
    /// </summary>
    [PgGucLabel("notice")]
    Notice = 18,

    /// <summary>
    /// The <c>warning</c> level.
    /// </summary>
    [PgGucLabel("warning")]
    Warning = 19,

    /// <summary>
    /// The <c>error</c> level.
    /// </summary>
    [PgGucLabel("error")]
    Error = 21,

    /// <summary>
    /// The <c>log</c> level.
    /// </summary>
    [PgGucLabel("log")]
    Log = 15,

    /// <summary>
    /// The <c>fatal</c> level.
    /// </summary>
    [PgGucLabel("fatal")]
    Fatal = 22,

    /// <summary>
    /// The <c>panic</c> level.
    /// </summary>
    [PgGucLabel("panic")]
    Panic = 23,
}
