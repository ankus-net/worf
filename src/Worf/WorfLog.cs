namespace Worf;

/// <summary>
/// Writes worf's messages to the server log as <see cref="PgLog"/> does, showing the bytes of a SQL_ASCII name that
/// aren't valid UTF-8, which the name's string holds as unpaired surrogates, as U+FFFD, as a <see cref="PgException"/>
/// shows them.
/// </summary>
internal static class WorfLog
{
    /// <summary>
    /// Writes a message at DEBUG1.
    /// </summary>
    /// <param name="message">The message.</param>
    internal static void Debug1(string message) => PgLog.Debug1(Worf.Shown(message));

    /// <summary>
    /// Writes a message at DEBUG2.
    /// </summary>
    /// <param name="message">The message.</param>
    internal static void Debug2(string message) => PgLog.Debug2(Worf.Shown(message));

    /// <summary>
    /// Writes a message at DEBUG3.
    /// </summary>
    /// <param name="message">The message.</param>
    internal static void Debug3(string message) => PgLog.Debug3(Worf.Shown(message));

    /// <summary>
    /// Writes a message at INFO.
    /// </summary>
    /// <param name="message">The message.</param>
    internal static void Info(string message) => PgLog.Info(Worf.Shown(message));

    /// <summary>
    /// Writes a message at LOG.
    /// </summary>
    /// <param name="message">The message.</param>
    internal static void Log(string message) => PgLog.Log(Worf.Shown(message));

    /// <summary>
    /// Writes a message at NOTICE.
    /// </summary>
    /// <param name="message">The message.</param>
    internal static void Notice(string message) => PgLog.Notice(Worf.Shown(message));

    /// <summary>
    /// Writes a message at WARNING.
    /// </summary>
    /// <param name="message">The message.</param>
    internal static void Warning(string message) => PgLog.Warning(Worf.Shown(message));

    /// <summary>
    /// Writes a message with its details at DEBUG1.
    /// </summary>
    /// <param name="diagnostic">The message and its details.</param>
    internal static void Debug1(PgDiagnostic diagnostic) => Write(PgLogLevel.Debug1, diagnostic);

    /// <summary>
    /// Writes a message with its details at DEBUG2.
    /// </summary>
    /// <param name="diagnostic">The message and its details.</param>
    internal static void Debug2(PgDiagnostic diagnostic) => Write(PgLogLevel.Debug2, diagnostic);

    /// <summary>
    /// Writes a message with its details at DEBUG3.
    /// </summary>
    /// <param name="diagnostic">The message and its details.</param>
    internal static void Debug3(PgDiagnostic diagnostic) => Write(PgLogLevel.Debug3, diagnostic);

    /// <summary>
    /// Writes a message with its details at INFO.
    /// </summary>
    /// <param name="diagnostic">The message and its details.</param>
    internal static void Info(PgDiagnostic diagnostic) => Write(PgLogLevel.Info, diagnostic);

    /// <summary>
    /// Writes a message with its details at LOG.
    /// </summary>
    /// <param name="diagnostic">The message and its details.</param>
    internal static void Log(PgDiagnostic diagnostic) => Write(PgLogLevel.Log, diagnostic);

    /// <summary>
    /// Writes a message with its details at NOTICE.
    /// </summary>
    /// <param name="diagnostic">The message and its details.</param>
    internal static void Notice(PgDiagnostic diagnostic) => Write(PgLogLevel.Notice, diagnostic);

    /// <summary>
    /// Writes a message with its details at WARNING.
    /// </summary>
    /// <param name="diagnostic">The message and its details.</param>
    internal static void Warning(PgDiagnostic diagnostic) => Write(PgLogLevel.Warning, diagnostic);

    /// <summary>
    /// Writes a message at a level.
    /// </summary>
    /// <param name="level">The level.</param>
    /// <param name="message">The message.</param>
    internal static void Write(PgLogLevel level, string message) => PgLog.Write(level, Worf.Shown(message));

    /// <summary>
    /// Writes a message with its details at a level.
    /// </summary>
    /// <param name="level">The level.</param>
    /// <param name="diagnostic">The message and its details.</param>
    internal static void Write(PgLogLevel level, PgDiagnostic diagnostic)
        => PgLog.Write(level, new PgDiagnostic(Worf.Shown(diagnostic.Message))
        {
            SqlState = diagnostic.SqlState,
            Detail = Worf.Shown(diagnostic.Detail),
            DetailLog = Worf.Shown(diagnostic.DetailLog),
            Hint = Worf.Shown(diagnostic.Hint),
            Context = Worf.Shown(diagnostic.Context),
            SchemaName = Worf.Shown(diagnostic.SchemaName),
            TableName = Worf.Shown(diagnostic.TableName),
            ColumnName = Worf.Shown(diagnostic.ColumnName),
            DataTypeName = Worf.Shown(diagnostic.DataTypeName),
            ConstraintName = Worf.Shown(diagnostic.ConstraintName),
        });
}
