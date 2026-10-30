using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Npgsql;

namespace Worf.CreateSubscriber;

/// <summary>
/// Opens Npgsql connections from libpq connection strings, as <c>PQconnectdb</c> and <c>PQping</c> do.
/// </summary>
internal static class Libpq
{
    /// <summary>
    /// The longest connection timeout Npgsql takes, in seconds.
    /// </summary>
    private const int NpgsqlTimeoutLimit = 1024;

    /// <summary>
    /// The longest one timer waits.
    /// </summary>
    private static readonly TimeSpan s_longestTimer = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private static readonly string[] s_targetSessionAttrs = ["any", "read-write", "read-only", "primary", "standby", "prefer-standby"];

    /// <summary>
    /// Opens a connection.
    /// </summary>
    /// <param name="conninfo">A libpq connection string or URI.</param>
    /// <returns>The open connection.</returns>
    /// <exception cref="ArgumentException">The connection string is invalid.</exception>
    /// <exception cref="NpgsqlException">The connection failed.</exception>
    public static NpgsqlConnection Connect(string conninfo)
        => TryConnect(conninfo, out _, out NpgsqlException? error) ?? throw error!;

    /// <summary>
    /// Reports whether a server accepts connections, as <c>PQping</c> does.
    /// </summary>
    /// <param name="conninfo">A libpq connection string or URI.</param>
    /// <returns>The server's status.</returns>
    public static PingStatus Ping(string conninfo)
    {
        try
        {
            using NpgsqlConnection? connection = TryConnect(conninfo, out PingStatus status, out _);
            return status;
        }
        catch (ArgumentException)
        {
            return PingStatus.NoAttempt;
        }
    }

    /// <summary>
    /// Converts a libpq connection string or URI to Npgsql's settings for every server, with libpq's environment
    /// variables and defaults, and lists the servers to try in turn.
    /// </summary>
    /// <param name="conninfo">The libpq connection string.</param>
    /// <param name="servers">The hosts or Unix-domain socket directories, with their ports.</param>
    /// <param name="connectTimeout">The seconds to wait for each address, or zero to wait indefinitely.</param>
    /// <param name="targetSessionAttrs">The kind of session to accept.</param>
    /// <returns>The Npgsql connection string builder.</returns>
    /// <exception cref="ArgumentException">The connection string is invalid.</exception>
    public static NpgsqlConnectionStringBuilder ToNpgsql(string conninfo, out List<(string Host, int Port)> servers,
        out int connectTimeout, out string targetSessionAttrs)
    {
        Dictionary<string, string> options = Fe.ConninfoParse(conninfo, out string? errmsg)
            ?? throw new ArgumentException(errmsg, nameof(conninfo));

        string? Option(string keyword, string? environment = null)
            => options.TryGetValue(keyword, out string? value) && value.Length > 0
                ? value
                : environment is not null && Environment.GetEnvironmentVariable(environment) is { Length: > 0 } fromEnvironment
                    ? fromEnvironment
                    : null;

        string user = Option("user", "PGUSER") ?? Environment.UserName;
        NpgsqlConnectionStringBuilder builder = new()
        {
            Username = user,
            Database = Option("dbname", "PGDATABASE") ?? user,
            Pooling = false,

            // libpq waits as long as connecting and each command take, where Npgsql gives up after 15 and 30 seconds:
            // the first connection to a node that has just been promoted can take longer on a busy machine.
            Timeout = 0,
            CommandTimeout = 0,

            // The command has no GSSAPI, as a libpq built without it, so it doesn't ask for GSS encryption, which Npgsql
            // would otherwise try every failed connection again without.
            GssEncryptionMode = GssEncryptionMode.Disable,
        };

        // As in libpq, a list of hosts, of addresses, or of both of the same length, with one port for all of them or
        // one each.
        string[] hosts = List(Option("host", "PGHOST"));
        string[] hostaddrs = List(Option("hostaddr", "PGHOSTADDR"));
        string[] ports = List(Option("port", "PGPORT"));
        int count = hostaddrs.Length > 0 ? hostaddrs.Length : Math.Max(hosts.Length, 1);
        if (hosts.Length > 0 && hostaddrs.Length > 0 && hosts.Length != hostaddrs.Length)
        {
            throw new ArgumentException($"could not match {hosts.Length} host names to {hostaddrs.Length} hostaddr values", nameof(conninfo));
        }

        if (ports.Length > 1 && ports.Length != count)
        {
            throw new ArgumentException($"could not match {ports.Length} port numbers to {count} hosts", nameof(conninfo));
        }

        servers = [];
        for (int i = 0; i < count; i++)
        {
            string portText = ports.Length == 0 ? "" : ports[ports.Length == 1 ? 0 : i];
            int port = portText.Length == 0 ? 5432 : ParseInt(portText, "port");
            if (port is < 1 or > 65535)
            {
                throw new ArgumentException($"invalid port number: \"{portText}\"", nameof(conninfo));
            }

            string host = hosts.Length > 0 && hosts[i].Length > 0 ? hosts[i]
                : hostaddrs.Length > 0 && hostaddrs[i].Length > 0 ? hostaddrs[i]
                : DefaultHost(port);
            servers.Add((host, port));
        }

        if (Option("password", "PGPASSWORD") is { } password)
        {
            builder.Password = password;
        }

        if (Option("passfile", "PGPASSFILE") is { } passfile)
        {
            builder.Passfile = passfile;
        }

        // As in libpq, zero or less waits indefinitely and the shortest timeout is 2 seconds.
        connectTimeout = 0;
        if (Option("connect_timeout", "PGCONNECT_TIMEOUT") is { } timeout)
        {
            int seconds = ParseInt(timeout, "connect_timeout");
            connectTimeout = seconds <= 0 ? 0 : Math.Max(seconds, 2);
        }

        targetSessionAttrs = Option("target_session_attrs", "PGTARGETSESSIONATTRS") ?? "any";
        if (!s_targetSessionAttrs.Contains(targetSessionAttrs))
        {
            throw new ArgumentException($"invalid target_session_attrs value: \"{targetSessionAttrs}\"", nameof(conninfo));
        }

        if ((Option("application_name", "PGAPPNAME") ?? Option("fallback_application_name")) is { } applicationName)
        {
            builder.ApplicationName = applicationName;
        }

        if (Option("options", "PGOPTIONS") is { } serverOptions)
        {
            builder.Options = serverOptions;
        }

        if (Option("sslmode", "PGSSLMODE") is { } sslmode)
        {
            builder.SslMode = sslmode switch
            {
                "disable" => SslMode.Disable,
                "allow" => SslMode.Allow,
                "prefer" => SslMode.Prefer,
                "require" => SslMode.Require,
                "verify-ca" => SslMode.VerifyCA,
                "verify-full" => SslMode.VerifyFull,
                _ => throw new ArgumentException($"invalid sslmode value: \"{sslmode}\"", nameof(conninfo)),
            };
        }

        if (Option("sslcert", "PGSSLCERT") is { } sslcert)
        {
            builder.SslCertificate = sslcert;
        }

        if (Option("sslkey", "PGSSLKEY") is { } sslkey)
        {
            builder.SslKey = sslkey;
        }

        if (Option("sslpassword") is { } sslpassword)
        {
            builder.SslPassword = sslpassword;
        }

        if (Option("sslrootcert", "PGSSLROOTCERT") is { } sslrootcert)
        {
            builder.RootCertificate = sslrootcert;
        }

        if (Option("keepalives_idle") is { } keepalivesIdle)
        {
            builder.TcpKeepAliveTime = ParseInt(keepalivesIdle, "keepalives_idle");
        }

        if (Option("keepalives_interval") is { } keepalivesInterval)
        {
            builder.TcpKeepAliveInterval = ParseInt(keepalivesInterval, "keepalives_interval");
        }

        return builder;
    }

    /// <summary>
    /// Tries each server in turn until one gives a suitable session, as libpq does: connect_timeout bounds each address
    /// of each server on its own, and one that times out, fails or gives the wrong kind of session leaves the next to
    /// try.
    /// </summary>
    /// <param name="conninfo">A libpq connection string or URI.</param>
    /// <param name="status">What the last server tried showed of itself, as <c>PQping</c> reports it.</param>
    /// <param name="error">Why each server failed, when none was suitable.</param>
    /// <returns>The open connection, or null.</returns>
    /// <exception cref="ArgumentException">The connection string is invalid.</exception>
    private static NpgsqlConnection? TryConnect(string conninfo, out PingStatus status, out NpgsqlException? error)
    {
        NpgsqlConnectionStringBuilder builder = ToNpgsql(conninfo, out List<(string Host, int Port)> servers,
            out int connectTimeout, out string targetSessionAttrs);

        // A standby is preferred over any server, so the servers are tried for one before any server will do.
        string[] passes = targetSessionAttrs == "prefer-standby" ? ["standby", "any"] : [targetSessionAttrs];
        List<string> failures = [];
        Exception? last = null;
        status = PingStatus.NoResponse;
        foreach (string wanted in passes)
        {
            foreach ((string host, int port) in servers)
            {
                builder.Host = host;
                builder.Port = port;
                NpgsqlConnection? connection;
                string? reason;
                try
                {
                    connection = OpenSuitable(builder, connectTimeout, wanted, out reason);
                }
                catch (NpgsqlException failure)
                {
                    status = failure switch
                    {
                        // The server is starting up, shutting down or in crash recovery.
                        PostgresException { SqlState: PostgresErrorCodes.CannotConnectNow } => PingStatus.Reject,

                        // The server answered, so it's up even though it refused this connection.
                        PostgresException => PingStatus.Ok,
                        _ => PingStatus.NoResponse,
                    };
                    failures.Add($"{Server(host, port)} failed: {Reason(failure)}");
                    last = failure;
                    continue;
                }

                status = PingStatus.Ok;
                if (connection is not null)
                {
                    error = null;
                    return connection;
                }

                failures.Add($"{Server(host, port)} failed: {reason}");
                last = null;
            }
        }

        error = new NpgsqlException(string.Join('\n', failures), last);
        return null;
    }

    /// <summary>
    /// Connects to one server, keeping the connection if its session is the kind wanted.
    /// </summary>
    /// <param name="builder">The connection string, with the server.</param>
    /// <param name="connectTimeout">The seconds to wait for each of the server's addresses, or zero to wait
    /// indefinitely.</param>
    /// <param name="wanted">The kind of session wanted.</param>
    /// <param name="reason">Why the session isn't the kind wanted, in libpq's words.</param>
    /// <returns>The open connection, or null when its session isn't the kind wanted.</returns>
    private static NpgsqlConnection? OpenSuitable(NpgsqlConnectionStringBuilder builder, int connectTimeout, string wanted,
        out string? reason)
    {
        NpgsqlConnection connection = Open(builder, connectTimeout);
        try
        {
            reason = Unsuitable(connection, wanted);
        }
        catch
        {
            connection.Dispose();
            throw;
        }

        if (reason is null)
        {
            return connection;
        }

        connection.Dispose();
        return null;
    }

    /// <summary>
    /// Names a server as libpq's messages do.
    /// </summary>
    private static string Server(string host, int port)
        => IsUnixSocket(host)
            ? $"connection to server on socket \"{Path.Join(host, $".s.PGSQL.{port.ToString(CultureInfo.InvariantCulture)}")}\""
            : $"connection to server at \"{host}\", port {port.ToString(CultureInfo.InvariantCulture)}";

    /// <summary>
    /// Connects to one server.
    /// </summary>
    /// <param name="builder">The connection string, with the server.</param>
    /// <param name="connectTimeout">The seconds to wait for each of the server's addresses, or zero to wait
    /// indefinitely.</param>
    /// <returns>The open connection.</returns>
    private static NpgsqlConnection Open(NpgsqlConnectionStringBuilder builder, int connectTimeout)
    {
        // Npgsql shares a server's timeout among its addresses, where libpq gives each its own, so the server gets as
        // much as all its addresses would. Npgsql takes no timeout longer than its limit, and sets a socket's timeout in
        // whole milliseconds, which leaves a read with less than one to go waiting forever, so a timer of its own bounds
        // the wait as well.
        long seconds = (long)connectTimeout * Addresses(builder.Host!);
        builder.Timeout = seconds <= NpgsqlTimeoutLimit ? (int)seconds : 0;
        // The slim builder leaves TLS out unless asked, and then any sslmode but disable, allow and prefer fails.
        NpgsqlDataSource dataSource = new NpgsqlSlimDataSourceBuilder(builder.ConnectionString).EnableTransportSecurity().Build();
        try
        {
            return seconds == 0
                ? dataSource.OpenConnection()
                : OpenAsync(dataSource, TimeSpan.FromSeconds(seconds)).GetAwaiter().GetResult();
        }
        catch
        {
            dataSource.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Connects, giving up after a timeout.
    /// </summary>
    /// <param name="dataSource">The server's data source.</param>
    /// <param name="timeout">The timeout.</param>
    /// <returns>The open connection.</returns>
    private static async Task<NpgsqlConnection> OpenAsync(NpgsqlDataSource dataSource, TimeSpan timeout)
    {
        using CancellationTokenSource expired = new();
        using CancellationTokenSource opened = new();
        Task<NpgsqlConnection> open = dataSource.OpenConnectionAsync(expired.Token).AsTask();

        // libpq takes timeouts of up to 68 years, and a timer waits at most 49 days, so a longer one waits again.
        long start = Stopwatch.GetTimestamp();
        for (TimeSpan left = timeout; left > TimeSpan.Zero; left = timeout - Stopwatch.GetElapsedTime(start))
        {
            if (await Task.WhenAny(open, Task.Delay(left < s_longestTimer ? left : s_longestTimer, opened.Token)) == open)
            {
                break;
            }
        }

        await opened.CancelAsync();
        if (!open.IsCompleted)
        {
            await expired.CancelAsync();
        }

        try
        {
            return await open;
        }
        catch (Exception error) when (expired.IsCancellationRequested && error is OperationCanceledException or NpgsqlException)
        {
            throw new NpgsqlException("timeout expired", error);
        }
    }

    /// <summary>
    /// Counts the addresses Npgsql tries for a host: one for a Unix-domain socket or an address, or each its name
    /// resolves to.
    /// </summary>
    private static int Addresses(string host)
    {
        if (IsUnixSocket(host) || IPAddress.TryParse(host, out _))
        {
            return 1;
        }

        try
        {
            return Math.Max(Dns.GetHostAddresses(host).Length, 1);
        }
        catch (SocketException)
        {
            // Npgsql reports the failure when it resolves the name itself.
            return 1;
        }
    }

    /// <summary>
    /// Says why a session isn't the kind wanted, in libpq's words, from the settings the server reports.
    /// </summary>
    private static string? Unsuitable(NpgsqlConnection connection, string wanted)
    {
        bool hotStandby = Reports(connection, "in_hot_standby");
        bool readOnly = hotStandby || Reports(connection, "default_transaction_read_only");
        return wanted switch
        {
            "read-write" when readOnly => "session is read-only",
            "read-only" when !readOnly => "session is not read-only",
            "primary" when hotStandby => "server is in hot standby mode",
            "standby" when !hotStandby => "server is not in hot standby mode",
            _ => null,
        };

        static bool Reports(NpgsqlConnection connection, string parameter)
            => connection.PostgresParameters.TryGetValue(parameter, out string? value) && value == "on";
    }

    /// <summary>
    /// Says why a connection failed, as libpq does.
    /// </summary>
    private static string Reason(NpgsqlException failure)
    {
        if (failure is PostgresException postgres)
        {
            return $"{postgres.Severity}:  {postgres.MessageText}";
        }

        for (Exception? cause = failure; cause is not null; cause = cause.InnerException)
        {
            switch (cause)
            {
                case TimeoutException or OperationCanceledException:
                    return "timeout expired";
                case SocketException socket:
                    return socket.Message;
            }
        }

        return failure.Message;
    }

    /// <summary>
    /// Parses an integer option as libpq does: with any whitespace around it, failing on anything else or an overflow.
    /// </summary>
    private static int ParseInt(string value, string keyword)
        => int.TryParse(value, NumberStyles.AllowLeadingWhite | NumberStyles.AllowTrailingWhite | NumberStyles.AllowLeadingSign,
            CultureInfo.InvariantCulture, out int result)
            ? result
            : throw new ArgumentException($"invalid integer value \"{value}\" for connection option \"{keyword}\"", nameof(value));

    /// <summary>
    /// Splits a comma-separated list of hosts, addresses or ports, as libpq does.
    /// </summary>
    private static string[] List(string? value) => value is null ? [] : value.Split(',');

    /// <summary>
    /// Says whether a host is a Unix-domain socket's directory, as libpq does.
    /// </summary>
    private static bool IsUnixSocket(string host) => Path.IsPathRooted(host) || host.StartsWith('@');

    /// <summary>
    /// Chooses the host libpq uses when none is given: a Unix-domain socket where the server's is, or else localhost.
    /// </summary>
    private static string DefaultHost(int port)
    {
        if (OperatingSystem.IsWindows())
        {
            return "localhost";
        }

        string[] directories = ["/tmp", "/var/run/postgresql"];
        foreach (string directory in directories)
        {
            if (File.Exists(Path.Join(directory, $".s.PGSQL.{port.ToString(CultureInfo.InvariantCulture)}")))
            {
                return directory;
            }
        }

        return "/tmp";
    }
}
