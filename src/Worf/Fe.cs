using System.Globalization;
using System.Text;
#if WORF_FRONTEND
using System.ComponentModel;
using System.Diagnostics;
#else
using Ankus.Postgres;
using Path = System.IO.Path;
#endif

namespace Worf;

/// <summary>
/// worf utility functions shared between the extension and <c>worf_create_subscriber</c>.
/// </summary>
internal static class Fe
{
    /// <summary>
    /// The connection options libpq accepts, in the order its option table lists them.
    /// </summary>
    private static readonly string[] s_conninfoOptions =
    [
        "service", "user", "password", "passfile", "channel_binding", "connect_timeout", "dbname", "host", "hostaddr",
        "port", "client_encoding", "options", "application_name", "fallback_application_name", "keepalives",
        "keepalives_idle", "keepalives_interval", "keepalives_count", "tcp_user_timeout", "sslmode", "sslnegotiation",
        "sslcompression", "sslcert", "sslkey", "sslkeylogfile", "sslcertmode", "sslpassword", "sslrootcert", "sslcrl",
        "sslcrldir", "sslsni", "requirepeer", "require_auth", "min_protocol_version", "max_protocol_version",
        "ssl_min_protocol_version", "ssl_max_protocol_version", "gssencmode", "krbsrvname", "gsslib", "gssdelegation",
        "replication", "target_session_attrs", "load_balance_hosts", "scram_client_key", "scram_server_key",
        "oauth_issuer", "oauth_client_id", "oauth_client_secret", "oauth_scope", "servicefile",
    ];

    /// <summary>
    /// Find another program in our binary's directory, and return its version.
    /// </summary>
    /// <param name="argv0">The path of our own binary.</param>
    /// <param name="target">The other program's name, without its suffix.</param>
    /// <param name="version">The other program's major version, such as 180000 for 18.6.</param>
    /// <param name="retpath">The other program's path.</param>
    /// <returns>Zero on success, -1 when the program can't be run and -2 when its version can't be read.</returns>
    internal static int FindOtherExecVersion(string argv0, string target, out uint version, out string retpath)
    {
        version = 0;

        // Trim off program name and keep just directory, then append the other program's name.
        string? directory = Path.GetDirectoryName(Path.GetFullPath(argv0));
        if (directory is null)
        {
            retpath = "";
            return -1;
        }

        retpath = Path.Join(directory, target + (OperatingSystem.IsWindows() ? ".exe" : ""));

        // And request the version from the program
        string? cmdOutput = ReadVersionLine(retpath);
        if (cmdOutput is null)
        {
            Console.Error.WriteLine($"find_other_exec_version: couldn't read output of \"\"{retpath}\" --version\"");
            return -1;
        }

        // Skip the program name and "(PostgreSQL)", then read the version's first two numbers.
        string[] words = cmdOutput.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        if (words.Length < 3 || !TryScanVersion(words[2], out int preDot, out int postDot))
        {
            Console.Error.WriteLine($"find_other_exec_version: couldn't scan result \"{cmdOutput}\" as version");
            return -2;
        }

        // similar to version number exposed by "server_version_num" but without the minor:
        // 9.6.1 -> 90601 -> 90600, 10.1 -> 100001 -> 100000
        version = (uint)(preDot < 10 ? (preDot * 100 + postDot) * 100 : preDot * 100 * 100);
        return 0;
    }

    /// <summary>
    /// Build connection string from individual parameter. dbname can be specified in connstr parameter.
    /// </summary>
    /// <param name="connstr">A connection string or URI, or a database name.</param>
    /// <param name="dbname">The database name, or null.</param>
    /// <param name="options">The <c>options</c> parameter, or null.</param>
    /// <param name="errmsg">The parse error when the result is null.</param>
    /// <returns>The connection string, or null when <paramref name="connstr"/> can't be parsed.</returns>
    internal static string? GetConnstr(string? connstr, string? dbname, string? options, out string? errmsg)
    {
        errmsg = null;
        List<(string Keyword, string Value)> parameters = [];

        // Merge the connection info inputs given in form of connection string and options
        if (connstr is not null &&
            (connstr.StartsWith("postgresql://", StringComparison.Ordinal) ||
             connstr.StartsWith("postgres://", StringComparison.Ordinal) ||
             connstr.Contains('=', StringComparison.Ordinal)))
        {
            Dictionary<string, string>? connOpts = ConninfoParse(connstr, out errmsg);
            if (connOpts is null)
            {
                return null;
            }

            foreach (string keyword in s_conninfoOptions)
            {
                // If db* parameters were provided, we'll fill them later.
                if (dbname is not null && keyword == "dbname")
                {
                    continue;
                }

                if (connOpts.TryGetValue(keyword, out string? value) && value.Length != 0)
                {
                    parameters.Add((keyword, value));
                }
            }
        }
        else if (connstr is not null && dbname is null)
        {
            // If connstr was provided but it's not in connection string format and the dbname wasn't provided then
            // connstr is actually dbname.
            dbname = connstr;
        }

        if (dbname is not null)
        {
            parameters.Add(("dbname", dbname));
        }

        if (options is not null)
        {
            parameters.Add(("options", options));
        }

        // Convert the parameters into conninfo string
        StringBuilder ret = new();
        for (int i = 0; i < parameters.Count; i++)
        {
            if (i > 0)
            {
                ret.Append(' ');
            }

            ret.Append(parameters[i].Keyword).Append('=');
            AppendConnstrValue(ret, parameters[i].Value);
        }

        return ret.ToString();
    }

    /// <summary>
    /// Escape connection info value.
    /// </summary>
    /// <param name="buf">The connection string being built.</param>
    /// <param name="str">The value.</param>
    internal static void AppendConnstrValue(StringBuilder buf, string str)
    {
        // If the string consists entirely of plain ASCII characters, no need to quote it. This is quite
        // conservative, but better safe than sorry.
        bool needquotes = false;
        foreach (char s in str)
        {
            if (!((s >= 'a' && s <= 'z') || (s >= 'A' && s <= 'Z') || (s >= '0' && s <= '9') || s == '_' || s == '.'))
            {
                needquotes = true;
                break;
            }
        }

        if (needquotes)
        {
            buf.Append('\'');
            foreach (char s in str)
            {
                // ' and \ must be escaped by to \' and \\
                if (s is '\'' or '\\')
                {
                    buf.Append('\\');
                }

                buf.Append(s);
            }

            buf.Append('\'');
        }
        else
        {
            buf.Append(str);
        }
    }

    /// <summary>
    /// Parses a connection string or URI into its options, as libpq's <c>PQconninfoParse</c> does.
    /// </summary>
    /// <param name="conninfo">The connection string or URI.</param>
    /// <param name="errmsg">The error when the result is null.</param>
    /// <returns>The options, or null.</returns>
    internal static Dictionary<string, string>? ConninfoParse(string conninfo, out string? errmsg)
    {
        Dictionary<string, string> options = new(StringComparer.Ordinal);
        bool parsed = conninfo.StartsWith("postgresql://", StringComparison.Ordinal) ||
            conninfo.StartsWith("postgres://", StringComparison.Ordinal)
            ? ConninfoUriParse(conninfo, options, out errmsg)
            : ConninfoKeywordParse(conninfo, options, out errmsg);
        return parsed ? options : null;
    }

    private static bool ConninfoKeywordParse(string conninfo, Dictionary<string, string> options, out string? errmsg)
    {
        errmsg = null;
        int cp = 0;
        while (cp < conninfo.Length)
        {
            // Skip blanks before the parameter name
            if (char.IsWhiteSpace(conninfo[cp]))
            {
                cp++;
                continue;
            }

            // Get the parameter name
            int nameStart = cp;
            while (cp < conninfo.Length && conninfo[cp] != '=' && !char.IsWhiteSpace(conninfo[cp]))
            {
                cp++;
            }

            string pname = conninfo[nameStart..cp];

            // Check that there is a following '='
            while (cp < conninfo.Length && char.IsWhiteSpace(conninfo[cp]))
            {
                cp++;
            }

            if (cp >= conninfo.Length || conninfo[cp] != '=')
            {
                errmsg = $"missing \"=\" after \"{pname}\" in connection info string\n";
                return false;
            }

            cp++;

            // Skip blanks after the '='
            while (cp < conninfo.Length && char.IsWhiteSpace(conninfo[cp]))
            {
                cp++;
            }

            // Get the parameter value
            StringBuilder pval = new();
            if (cp < conninfo.Length && conninfo[cp] == '\'')
            {
                cp++;
                while (true)
                {
                    if (cp >= conninfo.Length)
                    {
                        errmsg = "unterminated quoted string in connection info string\n";
                        return false;
                    }

                    if (conninfo[cp] == '\\')
                    {
                        cp++;
                        if (cp < conninfo.Length)
                        {
                            pval.Append(conninfo[cp++]);
                        }

                        continue;
                    }

                    if (conninfo[cp] == '\'')
                    {
                        cp++;
                        break;
                    }

                    pval.Append(conninfo[cp++]);
                }
            }
            else
            {
                while (cp < conninfo.Length && !char.IsWhiteSpace(conninfo[cp]))
                {
                    if (conninfo[cp] == '\\')
                    {
                        cp++;
                        if (cp < conninfo.Length)
                        {
                            pval.Append(conninfo[cp++]);
                        }

                        continue;
                    }

                    pval.Append(conninfo[cp++]);
                }
            }

            if (!StoreOption(options, pname, pval.ToString(), out errmsg))
            {
                return false;
            }
        }

        return true;
    }

    private static bool ConninfoUriParse(string uri, Dictionary<string, string> options, out string? errmsg)
    {
        errmsg = null;
        int prefixLength = uri.StartsWith("postgresql://", StringComparison.Ordinal) ? "postgresql://".Length : "postgres://".Length;
        string rest = uri[prefixLength..];

        // Split off the parameters and the database.
        int question = rest.IndexOf('?', StringComparison.Ordinal);
        string? query = question >= 0 ? rest[(question + 1)..] : null;
        if (question >= 0)
        {
            rest = rest[..question];
        }

        // Look for user and password.
        int at = rest.IndexOf('@', StringComparison.Ordinal);
        int firstSlash = rest.IndexOf('/', StringComparison.Ordinal);
        if (at >= 0 && (firstSlash < 0 || at < firstSlash))
        {
            string userinfo = rest[..at];
            rest = rest[(at + 1)..];
            int colon = userinfo.IndexOf(':', StringComparison.Ordinal);
            string user = colon >= 0 ? userinfo[..colon] : userinfo;
            if (user.Length != 0 && !StoreDecodedOption(options, "user", user, uri, out errmsg))
            {
                return false;
            }

            if (colon >= 0 && !StoreDecodedOption(options, "password", userinfo[(colon + 1)..], uri, out errmsg))
            {
                return false;
            }
        }

        // Hosts and ports, up to the database.
        int slash = rest.IndexOf('/', StringComparison.Ordinal);
        string netloc = slash >= 0 ? rest[..slash] : rest;
        string? database = slash >= 0 ? rest[(slash + 1)..] : null;
        List<string> hosts = [];
        List<string> ports = [];
        foreach (string entry in netloc.Split(','))
        {
            string host;
            string port = "";
            if (entry.StartsWith('['))
            {
                int close = entry.IndexOf(']', StringComparison.Ordinal);
                if (close < 0)
                {
                    errmsg = $"end of string reached when looking for matching \"]\" in IPv6 host address in URI: \"{uri}\"\n";
                    return false;
                }

                host = entry[1..close];
                if (close + 1 < entry.Length)
                {
                    if (entry[close + 1] != ':')
                    {
                        errmsg = $"unexpected character \"{entry[close + 1]}\" at position {prefixLength + close + 2} in URI (expected \":\" or \"/\"): \"{uri}\"\n";
                        return false;
                    }

                    port = entry[(close + 2)..];
                }
            }
            else
            {
                int colon = entry.IndexOf(':', StringComparison.Ordinal);
                host = colon >= 0 ? entry[..colon] : entry;
                port = colon >= 0 ? entry[(colon + 1)..] : "";
            }

            hosts.Add(host);
            ports.Add(port);
        }

        if (hosts.Exists(static host => host.Length != 0) &&
            !StoreDecodedOption(options, "host", string.Join(',', hosts), uri, out errmsg))
        {
            return false;
        }

        if (ports.Exists(static port => port.Length != 0) &&
            !StoreDecodedOption(options, "port", string.Join(',', ports), uri, out errmsg))
        {
            return false;
        }

        if (!string.IsNullOrEmpty(database) && !StoreDecodedOption(options, "dbname", database, uri, out errmsg))
        {
            return false;
        }

        if (query is not null)
        {
            foreach (string parameter in query.Split('&'))
            {
                if (parameter.Length == 0)
                {
                    continue;
                }

                int equals = parameter.IndexOf('=', StringComparison.Ordinal);
                if (equals < 0)
                {
                    errmsg = $"missing key/value separator \"=\" in URI query parameter: \"{parameter}\"\n";
                    return false;
                }

                string? keyword = PercentDecode(parameter[..equals], uri, out errmsg);
                string? value = PercentDecode(parameter[(equals + 1)..], uri, out errmsg);
                if (keyword is null || value is null)
                {
                    return false;
                }

                // Special keyword handling for improved JDBC compatibility.
                if (keyword == "ssl" && value == "true")
                {
                    keyword = "sslmode";
                    value = "require";
                }

                if (!StoreOption(options, keyword, value, out errmsg))
                {
                    errmsg = $"invalid URI query parameter: \"{keyword}\"\n";
                    return false;
                }
            }
        }

        return true;
    }

    private static bool StoreDecodedOption(Dictionary<string, string> options, string keyword, string value, string uri, out string? errmsg)
    {
        string? decoded = PercentDecode(value, uri, out errmsg);
        return decoded is not null && StoreOption(options, keyword, decoded, out errmsg);
    }

    private static bool StoreOption(Dictionary<string, string> options, string keyword, string value, out string? errmsg)
    {
        if (Array.IndexOf(s_conninfoOptions, keyword) < 0)
        {
            errmsg = $"invalid connection option \"{keyword}\"\n";
            return false;
        }

        errmsg = null;
        options[keyword] = value;
        return true;
    }

    private static string? PercentDecode(string value, string uri, out string? errmsg)
    {
        errmsg = null;
        List<byte> bytes = [];
        for (int i = 0; i < value.Length; i++)
        {
            if (value[i] != '%')
            {
                bytes.AddRange(Encoding.UTF8.GetBytes(value[i].ToString()));
                continue;
            }

            if (i + 2 >= value.Length ||
                !byte.TryParse(value.AsSpan(i + 1, 2), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out byte decoded))
            {
                errmsg = $"invalid percent-encoded token: \"{value}\"\n";
                return null;
            }

            if (decoded == 0)
            {
                errmsg = $"forbidden value %00 in percent-encoded value: \"{value}\"\n";
                return null;
            }

            bytes.Add(decoded);
            i += 2;
        }

        _ = uri;
        return Encoding.UTF8.GetString([.. bytes]);
    }

    private static bool TryScanVersion(string text, out int preDot, out int postDot)
    {
        preDot = 0;
        postDot = 0;
        int index = 0;
        while (index < text.Length && char.IsAsciiDigit(text[index]))
        {
            preDot = preDot * 10 + (text[index++] - '0');
        }

        if (index == 0)
        {
            return false;
        }

        if (index < text.Length && text[index] == '.')
        {
            index++;
            while (index < text.Length && char.IsAsciiDigit(text[index]))
            {
                postDot = postDot * 10 + (text[index++] - '0');
            }
        }

        return true;
    }

#if WORF_FRONTEND
    private static string? ReadVersionLine(string path)
    {
        ProcessStartInfo start = new(path)
        {
            RedirectStandardOutput = true,
            UseShellExecute = false,
        };
        start.ArgumentList.Add("--version");
        try
        {
            using Process process = Process.Start(start)!;
            string? line = process.StandardOutput.ReadLine();
            process.WaitForExit();
            return line;
        }
        catch (Win32Exception)
        {
            return null;
        }
    }
#else
    private static unsafe string? ReadVersionLine(string path)
    {
        fixed (byte* cmd = Worf.ToServerCString($"\"{path}\" --version"))
        {
#if ANKUS_PG13 || ANKUS_PG14 || ANKUS_PG15 || ANKUS_PG16
            byte* line = stackalloc byte[1024];
            sbyte* result = NativeMethods.pipe_read_line((sbyte*)cmd, (sbyte*)line, 1024);
            return result is null ? null : Worf.FromServer(result);
#else
            sbyte* result = NativeMethods.pipe_read_line((sbyte*)cmd);
            if (result is null)
            {
                return null;
            }

            string line = Worf.FromServer(result);
            NativeMethods.pfree(result);
            return line;
#endif
        }
    }
#endif
}
