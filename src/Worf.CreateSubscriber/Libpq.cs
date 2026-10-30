using System.Runtime.InteropServices;
using System.Text;

namespace Worf.CreateSubscriber;

/// <summary>
/// Connects through libpq, as <c>pglogical_create_subscriber</c> does, so that connection strings, service files,
/// password files, environment variables, timeouts, TLS and GSSAPI work as in every other PostgreSQL program.
/// </summary>
internal static unsafe class Libpq
{
    // CONNECTION_OK and PGRES_COMMAND_OK and PGRES_TUPLES_OK from libpq-fe.h.
    private const int ConnectionOk = 0;
    private const int CommandOk = 1;
    private const int TuplesOk = 2;

    private static readonly Lock s_loading = new();
    private static bool s_loaded;
    private static delegate* unmanaged<byte*, nint> s_connectdb;
    private static delegate* unmanaged<nint, int> s_status;
    private static delegate* unmanaged<nint, byte*> s_errorMessage;
    private static delegate* unmanaged<nint, byte*, int> s_setClientEncoding;
    private static delegate* unmanaged<nint, void> s_finish;
    private static delegate* unmanaged<byte*, int> s_ping;
    private static delegate* unmanaged<nint, byte*, nint> s_exec;
    private static delegate* unmanaged<nint, int> s_resultStatus;
    private static delegate* unmanaged<nint, byte*> s_resultErrorMessage;
    private static delegate* unmanaged<nint, int> s_ntuples;
    private static delegate* unmanaged<nint, int> s_nfields;
    private static delegate* unmanaged<nint, int, int, byte*> s_getvalue;
    private static delegate* unmanaged<nint, int, int, int> s_getisnull;
    private static delegate* unmanaged<nint, void> s_clear;

    /// <summary>
    /// Opens a connection, as <c>PQconnectdb</c> does.
    /// </summary>
    /// <param name="conninfo">A libpq connection string or URI.</param>
    /// <returns>The open connection.</returns>
    /// <exception cref="LibpqException">The connection failed, with libpq's message.</exception>
    /// <exception cref="DllNotFoundException">There's no libpq to load.</exception>
    public static LibpqConnection Connect(string conninfo)
    {
        Load();
        nint conn;
        fixed (byte* cconninfo = CString(conninfo))
        {
            conn = s_connectdb(cconninfo);
        }

        if (conn == 0)
        {
            throw new LibpqException("out of memory\n");
        }

        if (s_status(conn) != ConnectionOk)
        {
            string message = String(s_errorMessage(conn));
            s_finish(conn);
            throw new LibpqException(message);
        }

        // Rows and messages come back as UTF-8, whatever the database's encoding.
        fixed (byte* encoding = CString("UTF8"))
        {
            _ = s_setClientEncoding(conn, encoding);
        }

        return new LibpqConnection(conn);
    }

    /// <summary>
    /// Reports whether a server accepts connections, as <c>PQping</c> does.
    /// </summary>
    /// <param name="conninfo">A libpq connection string or URI.</param>
    /// <returns>The server's status.</returns>
    /// <exception cref="DllNotFoundException">There's no libpq to load.</exception>
    public static PingStatus Ping(string conninfo)
    {
        Load();
        fixed (byte* cconninfo = CString(conninfo))
        {
            return (PingStatus)s_ping(cconninfo);
        }
    }

    /// <summary>
    /// Runs a command and reads every row it returns as text.
    /// </summary>
    /// <param name="conn">The connection.</param>
    /// <param name="sql">The command.</param>
    /// <returns>The rows.</returns>
    /// <exception cref="LibpqException">The command failed, with libpq's message.</exception>
    internal static List<string?[]> Exec(nint conn, string sql)
    {
        nint result;
        fixed (byte* query = CString(sql))
        {
            result = s_exec(conn, query);
        }

        try
        {
            if (s_resultStatus(result) is not (CommandOk or TuplesOk))
            {
                throw new LibpqException(String(result == 0 ? s_errorMessage(conn) : s_resultErrorMessage(result)));
            }

            int fields = s_nfields(result);
            List<string?[]> rows = [];
            for (int i = 0; i < s_ntuples(result); i++)
            {
                string?[] row = new string?[fields];
                for (int j = 0; j < fields; j++)
                {
                    row[j] = s_getisnull(result, i, j) != 0 ? null : String(s_getvalue(result, i, j));
                }

                rows.Add(row);
            }

            return rows;
        }
        finally
        {
            s_clear(result);
        }
    }

    /// <summary>
    /// Checks whether a connection is still good.
    /// </summary>
    /// <param name="conn">The connection.</param>
    /// <returns>Whether it is.</returns>
    internal static bool IsOk(nint conn) => s_status(conn) == ConnectionOk;

    /// <summary>
    /// Closes a connection.
    /// </summary>
    /// <param name="conn">The connection.</param>
    internal static void Finish(nint conn) => s_finish(conn);

    private static byte[] CString(string value) => Encoding.UTF8.GetBytes(value + '\0');

    private static string String(byte* value) => Marshal.PtrToStringUTF8((nint)value) ?? "";

    /// <summary>
    /// Loads the libpq of the PostgreSQL installation the command runs from, which PostgreSQL's own programs find
    /// through the run path built into them: the command's directory holds it on Windows, and on Unix the installation's
    /// lib directory does, beside the bin directory of the command or of the <c>pg_ctl</c> it runs. A libpq installed
    /// for the whole system serves when the installation has none of its own, as in Debian's packages.
    /// </summary>
    private static void Load()
    {
        lock (s_loading)
        {
            if (s_loaded)
            {
                return;
            }

            string file = OperatingSystem.IsWindows() ? "libpq.dll" : OperatingSystem.IsMacOS() ? "libpq.5.dylib" : "libpq.so.5";
            List<string> candidates = [];
            if (Environment.ProcessPath is { } command)
            {
                string[] programs = [command, Path.Join(Path.GetDirectoryName(command), OperatingSystem.IsWindows() ? "pg_ctl.exe" : "pg_ctl")];
                foreach (string program in programs.Where(File.Exists))
                {
                    string bin = Path.GetDirectoryName(File.ResolveLinkTarget(program, returnFinalTarget: true)?.FullName ?? program)!;
                    candidates.AddRange([Path.Join(bin, file), Path.Join(bin, "..", "lib", file), Path.Join(bin, "..", "lib", "postgresql", file)]);
                }
            }

            candidates.Add(file);
            nint library = 0;
            foreach (string candidate in candidates.Distinct())
            {
                if (NativeLibrary.TryLoad(candidate, out library))
                {
                    break;
                }
            }

            if (library == 0)
            {
                throw new DllNotFoundException($"{file} is in none of {string.Join(", ", candidates.Distinct())}");
            }

            s_connectdb = (delegate* unmanaged<byte*, nint>)NativeLibrary.GetExport(library, "PQconnectdb");
            s_status = (delegate* unmanaged<nint, int>)NativeLibrary.GetExport(library, "PQstatus");
            s_errorMessage = (delegate* unmanaged<nint, byte*>)NativeLibrary.GetExport(library, "PQerrorMessage");
            s_setClientEncoding = (delegate* unmanaged<nint, byte*, int>)NativeLibrary.GetExport(library, "PQsetClientEncoding");
            s_finish = (delegate* unmanaged<nint, void>)NativeLibrary.GetExport(library, "PQfinish");
            s_ping = (delegate* unmanaged<byte*, int>)NativeLibrary.GetExport(library, "PQping");
            s_exec = (delegate* unmanaged<nint, byte*, nint>)NativeLibrary.GetExport(library, "PQexec");
            s_resultStatus = (delegate* unmanaged<nint, int>)NativeLibrary.GetExport(library, "PQresultStatus");
            s_resultErrorMessage = (delegate* unmanaged<nint, byte*>)NativeLibrary.GetExport(library, "PQresultErrorMessage");
            s_ntuples = (delegate* unmanaged<nint, int>)NativeLibrary.GetExport(library, "PQntuples");
            s_nfields = (delegate* unmanaged<nint, int>)NativeLibrary.GetExport(library, "PQnfields");
            s_getvalue = (delegate* unmanaged<nint, int, int, byte*>)NativeLibrary.GetExport(library, "PQgetvalue");
            s_getisnull = (delegate* unmanaged<nint, int, int, int>)NativeLibrary.GetExport(library, "PQgetisnull");
            s_clear = (delegate* unmanaged<nint, void>)NativeLibrary.GetExport(library, "PQclear");
            s_loaded = true;
        }
    }
}
