using System.Globalization;
using System.Runtime.InteropServices;

namespace Worf;

/// <summary>
/// Writes the warnings, and the errors, that the servers this process connects to send as notices, as their functions
/// raise them, to this server's log, naming the server, rather than libpq's default, which writes every notice to
/// standard error without a time or process. Each connection is named as it's made: a node, when the connection is to
/// one this process exchanges with, or the server's host and port. Notices of lower severity, as a server's debug
/// messages are, aren't logged, and of a server's warnings, only the first ten each minute are. libpq's result fields
/// are part of its stable interface.
/// </summary>
internal static unsafe class WorfNotices
{
    // PG_DIAG_SEVERITY, PG_DIAG_SEVERITY_NONLOCALIZED, PG_DIAG_MESSAGE_PRIMARY and PG_DIAG_MESSAGE_DETAIL.
    private const int DiagSeverity = 'S';
    private const int DiagSeverityNonlocalized = 'V';
    private const int DiagMessagePrimary = 'M';
    private const int DiagMessageDetail = 'D';

    // How many of a server's notices are logged each minute.
    private const int PerMinute = 10;

    // The name of each connection's server, by its libpq connection.
    private static readonly Dictionary<nint, string> s_names = [];

    // When each server's minute began, and how many of its notices were logged in it.
    private static readonly Dictionary<string, (long Began, int Logged)> s_minutes = [];

    /// <summary>
    /// Writes a connection's notices to this server's log from now on, naming its server.
    /// </summary>
    /// <param name="conn">The libpq connection.</param>
    /// <param name="name">What the log calls the server, as <c>node node_b</c>.</param>
    internal static void Route(void* conn, string name)
    {
        s_names[(nint)conn] = name;
        var receiver = (delegate* unmanaged<void*, delegate* unmanaged<void*, void*, void>, void*, void*>)WorfConnection.LibpqFunction("PQsetNoticeReceiver");
        _ = receiver(conn, &Received, conn);
    }

    /// <summary>
    /// Writes a connection's notices to this server's log from now on, naming its server by its host and port.
    /// </summary>
    /// <param name="conn">The libpq connection.</param>
    internal static void RouteByAddress(void* conn)
    {
        string host = Worf.FromServer(((delegate* unmanaged<void*, sbyte*>)WorfConnection.LibpqFunction("PQhost"))(conn));
        string port = Worf.FromServer(((delegate* unmanaged<void*, sbyte*>)WorfConnection.LibpqFunction("PQport"))(conn));
        Route(conn, $"the server at {host}:{port}");
    }

    /// <summary>
    /// Forgets a connection that closes.
    /// </summary>
    /// <param name="conn">The libpq connection.</param>
    internal static void Forget(void* conn) => _ = s_names.Remove((nint)conn);

    // libpq calls this for each notice the server sends, as it reads the connection.
    [UnmanagedCallersOnly]
    private static void Received(void* arg, void* result)
    {
        var field = (delegate* unmanaged<void*, int, sbyte*>)WorfConnection.LibpqFunction("PQresultErrorField");
        sbyte* severity = field(result, DiagSeverity);
        sbyte* level = field(result, DiagSeverityNonlocalized);
        sbyte* primary = field(result, DiagMessagePrimary);
        sbyte* detail = field(result, DiagMessageDetail);
        string? kind = level is not null ? Worf.FromServer(level) : severity is not null ? Worf.FromServer(severity) : null;
        if (kind is not ("WARNING" or "ERROR" or "FATAL" or "PANIC"))
        {
            return;
        }

        string name = s_names.GetValueOrDefault((nint)arg) ?? "a server";
        long now = Worker.GetCurrentTimestamp();
        (long began, int logged) = s_minutes.GetValueOrDefault(name);
        if (now - began >= 60_000_000L)
        {
            (began, logged) = (now, 0);
        }

        s_minutes[name] = (began, logged + 1);
        try
        {
            if (logged < PerMinute)
            {
                WorfLog.Log(new PgDiagnostic($"{name} sent {(severity is null ? kind : Worf.FromServer(severity))}: {(primary is null ? "" : Worf.FromServer(primary))}")
                {
                    Detail = detail is null ? null : Worf.FromServer(detail),
                });
            }
            else if (logged == PerMinute)
            {
                WorfLog.Log($"{name} sent more than {PerMinute.ToString(CultureInfo.InvariantCulture)} warnings within a minute, the rest of which aren't logged");
            }
        }
        catch (PgException)
        {
            // A notice this server can't log is dropped, as libpq's default drops one it can't write.
        }
    }
}
