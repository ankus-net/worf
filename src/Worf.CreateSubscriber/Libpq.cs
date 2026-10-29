using System.Globalization;
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
    /// Opens a connection.
    /// </summary>
    /// <param name="conninfo">A libpq connection string or URI.</param>
    /// <returns>The open connection.</returns>
    /// <exception cref="ArgumentException">The connection string is invalid.</exception>
    /// <exception cref="NpgsqlException">The connection failed.</exception>
    public static NpgsqlConnection Connect(string conninfo)
    {
        NpgsqlDataSource dataSource = new NpgsqlSlimDataSourceBuilder(ToNpgsql(conninfo, out int connectTimeout)).Build();
        try
        {
            if (connectTimeout <= NpgsqlTimeoutLimit)
            {
                return dataSource.OpenConnection();
            }

            // libpq takes any timeout, but Npgsql none longer than its limit, so a longer one cancels a wait Npgsql
            // leaves unbounded, with libpq's message.
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(connectTimeout));
            try
            {
                return dataSource.OpenConnectionAsync(timeout.Token).AsTask().GetAwaiter().GetResult();
            }
            catch (OperationCanceledException error) when (timeout.IsCancellationRequested)
            {
                throw new NpgsqlException("timeout expired", error);
            }
        }
        catch
        {
            dataSource.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Reports whether a server accepts connections, as <c>PQping</c> does.
    /// </summary>
    /// <param name="conninfo">A libpq connection string or URI.</param>
    /// <returns>The server's status.</returns>
    public static PingStatus Ping(string conninfo)
    {
        try
        {
            using NpgsqlConnection connection = Connect(conninfo);
            return PingStatus.Ok;
        }
        catch (ArgumentException)
        {
            return PingStatus.NoAttempt;
        }
        catch (PostgresException error) when (error.SqlState == PostgresErrorCodes.CannotConnectNow)
        {
            // The server is starting up, shutting down or in crash recovery.
            return PingStatus.Reject;
        }
        catch (PostgresException)
        {
            // The server answered, so it's up even though it refused this connection.
            return PingStatus.Ok;
        }
        catch (NpgsqlException)
        {
            return PingStatus.NoResponse;
        }
    }

    /// <summary>
    /// Converts a libpq connection string or URI to an Npgsql connection string, with libpq's environment variables and
    /// defaults.
    /// </summary>
    /// <param name="conninfo">The libpq connection string.</param>
    /// <param name="connectTimeout">The seconds to wait while connecting, or zero to wait indefinitely.</param>
    /// <returns>The Npgsql connection string.</returns>
    /// <exception cref="ArgumentException">The connection string is invalid.</exception>
    public static string ToNpgsql(string conninfo, out int connectTimeout)
    {
        Dictionary<string, string> options = Fe.ConninfoParse(conninfo, out string? errmsg)
            ?? throw new ArgumentException(errmsg, nameof(conninfo));

        string? Option(string keyword, string? environment = null)
            => options.TryGetValue(keyword, out string? value) && value.Length > 0
                ? value
                : environment is not null && Environment.GetEnvironmentVariable(environment) is { Length: > 0 } fromEnvironment
                    ? fromEnvironment
                    : null;

        string port = Option("port", "PGPORT") ?? "5432";
        string user = Option("user", "PGUSER") ?? Environment.UserName;
        NpgsqlConnectionStringBuilder builder = new()
        {
            Host = Option("host", "PGHOST") ?? Option("hostaddr", "PGHOSTADDR") ?? DefaultHost(port),
            Port = int.Parse(port, CultureInfo.InvariantCulture),
            Username = user,
            Database = Option("dbname", "PGDATABASE") ?? user,
            Pooling = false,

            // libpq waits as long as connecting and each command take, where Npgsql gives up after 15 and 30 seconds:
            // the first connection to a node that has just been promoted can take longer on a busy machine.
            Timeout = 0,
            CommandTimeout = 0,
        };

        if (Option("password", "PGPASSWORD") is { } password)
        {
            builder.Password = password;
        }

        if (Option("passfile", "PGPASSFILE") is { } passfile)
        {
            builder.Passfile = passfile;
        }

        connectTimeout = 0;
        if (Option("connect_timeout", "PGCONNECT_TIMEOUT") is { } timeout)
        {
            // As in libpq, zero or less waits indefinitely and the shortest timeout is 2 seconds. Connect times one
            // longer than Npgsql takes itself.
            int seconds = int.Parse(timeout, CultureInfo.InvariantCulture);
            connectTimeout = seconds <= 0 ? 0 : Math.Max(seconds, 2);
            builder.Timeout = connectTimeout <= NpgsqlTimeoutLimit ? connectTimeout : 0;
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

        if (Option("target_session_attrs", "PGTARGETSESSIONATTRS") is { } targetSessionAttrs)
        {
            builder.TargetSessionAttributes = targetSessionAttrs;
        }

        if (Option("keepalives_idle") is { } keepalivesIdle)
        {
            builder.TcpKeepAliveTime = int.Parse(keepalivesIdle, CultureInfo.InvariantCulture);
        }

        if (Option("keepalives_interval") is { } keepalivesInterval)
        {
            builder.TcpKeepAliveInterval = int.Parse(keepalivesInterval, CultureInfo.InvariantCulture);
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// Chooses the host libpq uses when none is given: a Unix-domain socket where the server's is, or else localhost.
    /// </summary>
    private static string DefaultHost(string port)
    {
        if (OperatingSystem.IsWindows())
        {
            return "localhost";
        }

        string[] directories = ["/tmp", "/var/run/postgresql"];
        foreach (string directory in directories)
        {
            if (File.Exists(Path.Join(directory, $".s.PGSQL.{port}")))
            {
                return directory;
            }
        }

        return "/tmp";
    }
}
