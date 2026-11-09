using System.Runtime.InteropServices;

namespace Worf;

/// <summary>
/// Writes what the servers this process connects to send as notices, as warnings their functions raise, to this
/// server's log, naming the server, rather than libpq's default, which writes them to standard error without a time or
/// process. Each connection is named as it's made: a node, when the connection is to one this process exchanges with,
/// or the server's host and port. libpq's result fields are part of its stable interface.
/// </summary>
internal static unsafe class WorfNotices
{
    // PG_DIAG_SEVERITY, PG_DIAG_MESSAGE_PRIMARY and PG_DIAG_MESSAGE_DETAIL.
    private const int DiagSeverity = 'S';
    private const int DiagMessagePrimary = 'M';
    private const int DiagMessageDetail = 'D';

    // The name of each connection's server, by its libpq connection.
    private static readonly Dictionary<nint, string> s_names = [];

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
        sbyte* primary = field(result, DiagMessagePrimary);
        sbyte* detail = field(result, DiagMessageDetail);
        string name = s_names.GetValueOrDefault((nint)arg) ?? "a server";
        try
        {
            WorfLog.Log(new PgDiagnostic($"{name} sent {(severity is null ? "NOTICE" : Worf.FromServer(severity))}: {(primary is null ? "" : Worf.FromServer(primary))}")
            {
                Detail = detail is null ? null : Worf.FromServer(detail),
            });
        }
        catch (PgException)
        {
            // A notice this server can't log is dropped, as libpq's default drops one it can't write.
        }
    }
}
