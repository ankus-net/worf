using System.Text;
using Ankus.Postgres;

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

/// <summary>
/// worf initialization and common functionality.
/// </summary>
internal static partial class Worf
{
    /// <summary>
    /// The extension version.
    /// </summary>
    internal const string Version = "1.0.0";

    /// <summary>
    /// The extension version as a number.
    /// </summary>
    internal const int VersionNum = 10000;

    /// <summary>
    /// The oldest protocol version this worf speaks.
    /// </summary>
    internal const int MinProtoVersionNum = 1;

    /// <summary>
    /// The newest protocol version this worf speaks.
    /// </summary>
    internal const int MaxProtoVersionNum = 1;

    /// <summary>
    /// The name of the extension, its schema and its library.
    /// </summary>
    internal const string ExtensionName = "worf";

    /// <summary>
    /// The forward origin that forwards changes from every origin.
    /// </summary>
    internal const string ReplicationOriginAll = "all";

    private static string s_tempDirectory = "";

    /// <summary>
    /// Gets worf's synchronous commit value.
    /// </summary>
    [PgGucBool("worf.synchronous_commit", false, "worf specific synchronous commit value", Context = PgGucContext.Postmaster)]
    internal static partial bool SynchronousCommit { get; }

    /// <summary>
    /// Gets whether to apply changes through SPI instead of the low-level API.
    /// </summary>
    [PgGucBool("worf.use_spi", false, "Use SPI instead of low-level API for applying changes", Context = PgGucContext.Postmaster)]
    internal static partial bool UseSpi { get; }

    /// <summary>
    /// Gets whether to batch inserts if possible.
    /// </summary>
    [PgGucBool("worf.batch_inserts", true, "Batch inserts if possible", Context = PgGucContext.Postmaster)]
    internal static partial bool BatchInserts { get; }

    /// <summary>
    /// Gets the configured directory for dumps. We can't use the temp_tablespace safely for our dumps, because
    /// Pg's crash recovery is very careful to delete only particularly formatted files. Instead for now just allow
    /// user to specify dump storage.
    /// </summary>
    [PgGucString("worf.temp_directory", "", "Directory to store dumps for local restore", Context = PgGucContext.Sighup,
        Assign = nameof(TempDirectoryAssignHook))]
    internal static partial string TempDirectoryConfig { get; }

    /// <summary>
    /// Gets the connection options to add to all peer node connections.
    /// </summary>
    [PgGucString("worf.extra_connection_options", "", "connection options to add to all peer node connections",
        Context = PgGucContext.Sighup)]
    internal static partial string ExtraConnectionOptions { get; }

    /// <summary>
    /// Gets the directory to store dumps for local restore.
    /// </summary>
    internal static string TempDirectory => s_tempDirectory;

    /// <summary>
    /// Ensures a string is not longer than <paramref name="maxlen"/> bytes. If the string is longer we return a
    /// prefix of the string and the hash of the string, which together are exactly <paramref name="maxlen"/>
    /// bytes minus one, as snprintf leaves room for its terminator. Maxlen can't be less than 8 because the hash
    /// produces a uint32 which in hex form can have up to 8 characters.
    /// </summary>
    /// <param name="str">The string in the server encoding.</param>
    /// <param name="maxlen">The maximum length.</param>
    /// <returns>The shortened string in the server encoding.</returns>
    internal static byte[] ShortenHash(ReadOnlySpan<byte> str, int maxlen)
    {
        if (str.Length <= maxlen)
        {
            return str.ToArray();
        }

        uint hash = HashAny(str);
        byte[] hex = Encoding.ASCII.GetBytes(hash.ToString("x8", System.Globalization.CultureInfo.InvariantCulture));
        byte[] ret = new byte[maxlen - 1];
        str[..(maxlen - 8)].CopyTo(ret);
        hex.AsSpan(0, 7).CopyTo(ret.AsSpan(maxlen - 8));
        return ret;
    }

    /// <summary>
    /// Shortens a string as <see cref="ShortenHash(ReadOnlySpan{byte}, int)"/> does.
    /// </summary>
    /// <param name="str">The string.</param>
    /// <param name="maxlen">The maximum length in server encoding bytes.</param>
    /// <returns>The shortened string.</returns>
    internal static string ShortenHash(string str, int maxlen) => FromServer(ShortenHash(ToServer(str), maxlen));

    /// <summary>
    /// Hashes bytes with PostgreSQL's <c>hash_any</c>.
    /// </summary>
    /// <param name="data">The bytes.</param>
    /// <returns>The hash.</returns>
    internal static unsafe uint HashAny(ReadOnlySpan<byte> data)
    {
        fixed (byte* bytes = data)
        {
            return unchecked((uint)NativeMethods.hash_any(bytes, data.Length));
        }
    }

    /// <summary>
    /// Hashes a string's server-encoding bytes with PostgreSQL's <c>hash_any</c>, as
    /// <c>hash_any(str, strlen(str))</c> does.
    /// </summary>
    /// <param name="value">The string.</param>
    /// <returns>The hash.</returns>
    internal static uint HashAny(string value) => HashAny(ToServer(value));

    /// <summary>
    /// Converts a string to the bytes the database encoding uses for it.
    /// </summary>
    /// <param name="value">The string.</param>
    /// <returns>The bytes, without a terminator.</returns>
    internal static unsafe byte[] ToServer(string value)
    {
        byte[] utf8 = Encoding.UTF8.GetBytes(value);
        if (NativeMethods.GetDatabaseEncoding() == (int)pg_enc.PG_UTF8 || utf8.Length == 0)
        {
            return utf8;
        }

        fixed (byte* input = utf8)
        {
            sbyte* converted = NativeMethods.pg_any_to_server((sbyte*)input, utf8.Length, (int)pg_enc.PG_UTF8);
            if (converted == (sbyte*)input)
            {
                return utf8;
            }

            byte[] result = new ReadOnlySpan<byte>(converted, StrLen(converted)).ToArray();
            NativeMethods.pfree(converted);
            return result;
        }
    }

    /// <summary>
    /// Converts bytes in the database encoding to a string.
    /// </summary>
    /// <param name="value">The bytes, without a terminator.</param>
    /// <returns>The string.</returns>
    internal static unsafe string FromServer(ReadOnlySpan<byte> value)
    {
        if (NativeMethods.GetDatabaseEncoding() == (int)pg_enc.PG_UTF8 || value.Length == 0)
        {
            return Encoding.UTF8.GetString(value);
        }

        fixed (byte* input = value)
        {
            sbyte* converted = NativeMethods.pg_server_to_any((sbyte*)input, value.Length, (int)pg_enc.PG_UTF8);
            if (converted == (sbyte*)input)
            {
                return Encoding.UTF8.GetString(value);
            }

            string result = Encoding.UTF8.GetString(new ReadOnlySpan<byte>(converted, StrLen(converted)));
            NativeMethods.pfree(converted);
            return result;
        }
    }

    /// <summary>
    /// Converts a terminated C string in the database encoding to a string.
    /// </summary>
    /// <param name="value">The C string.</param>
    /// <returns>The string.</returns>
    internal static unsafe string FromServer(sbyte* value) => FromServer(new ReadOnlySpan<byte>(value, StrLen(value)));

    /// <summary>
    /// Converts a string to a terminated C string in the database encoding.
    /// </summary>
    /// <param name="value">The string.</param>
    /// <returns>The bytes, including the terminator.</returns>
    internal static byte[] ToServerCString(string value)
    {
        byte[] server = ToServer(value);
        byte[] result = new byte[server.Length + 1];
        server.CopyTo(result, 0);
        return result;
    }

    /// <summary>
    /// Reads a setting's display value, as <c>GetConfigOptionByName</c> returns it.
    /// </summary>
    /// <param name="name">The setting's name.</param>
    /// <returns>The value.</returns>
    internal static unsafe string? GetConfigOptionByName(string name)
    {
        fixed (byte* cname = ToServerCString(name))
        {
            sbyte* value = NativeMethods.GetConfigOptionByName((sbyte*)cname, null, false);
            return value is null ? null : FromServer(value);
        }
    }

    /// <summary>
    /// Converts a text array to a list of strings.
    /// </summary>
    /// <param name="textarray">The array.</param>
    /// <returns>The strings.</returns>
    internal static List<string> TextArrayToList(string?[] textarray)
    {
        List<string> res = [];
        foreach (string? element in textarray)
        {
            if (element is null)
            {
                throw new PgException(PgSqlStates.NullValueNotAllowed, "null array element not allowed in this context");
            }

            res.Add(element);
        }

        return res;
    }

    /// <summary>
    /// Deconstructs the text representation of a 1-dimensional Postgres array into individual items.
    /// </summary>
    /// <param name="atext">The array text, in the form <c>{item,item,item}</c> where any item is either raw data,
    /// or surrounded by double quotes (in which case embedded characters including backslashes and quotes are
    /// backslashed).</param>
    /// <param name="itemarray">The items on success.</param>
    /// <returns>Whether the text parsed.</returns>
    internal static bool ParsePGArray(string atext, out List<string> itemarray)
    {
        itemarray = [];
        int inputlen = atext.Length;
        if (inputlen < 2 || atext[0] != '{' || atext[inputlen - 1] != '}')
        {
            return false;
        }

        // A position past the end reads as the C string's terminator.
        char At(int index) => index < inputlen ? atext[index] : '\0';

        int position = 1;
        StringBuilder item = new();
        while (At(position) != '}')
        {
            if (At(position) == '\0')
            {
                return false;
            }

            item.Clear();
            while (At(position) != '}' && At(position) != ',')
            {
                if (At(position) == '\0')
                {
                    return false;
                }

                if (At(position) != '"')
                {
                    item.Append(atext[position++]);
                }
                else
                {
                    // Process quoted substring.
                    position++;
                    while (At(position) != '"')
                    {
                        if (At(position) == '\0')
                        {
                            return false;
                        }

                        if (At(position) == '\\')
                        {
                            position++;
                            if (At(position) == '\0')
                            {
                                return false;
                            }
                        }

                        item.Append(atext[position++]);
                    }

                    position++;
                }
            }

            itemarray.Add(item.ToString());
            if (At(position) == ',')
            {
                position++;
            }
        }

        // Bogus syntax (embedded '}').
        return At(position + 1) == '\0';
    }

    /// <summary>
    /// Gets the oid of one of our catalog tables.
    /// </summary>
    /// <param name="table">The table's name.</param>
    /// <returns>The table's oid.</returns>
    internal static uint GetWorfTableOid(string table)
    {
        uint? nspoid = Spi.ExecuteScalar<uint?>("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = $1",
            SpiParameter.Create(ExtensionName));
        if (nspoid is null)
        {
            throw new PgException(PgSqlStates.UndefinedSchema, $"schema \"{ExtensionName}\" does not exist");
        }

        uint? reloid = Spi.ExecuteScalar<uint?>(
            "SELECT oid FROM pg_catalog.pg_class WHERE relname = $1 AND relnamespace = $2",
            SpiParameter.Create(table), SpiParameter.Create(nspoid.Value));
        if (reloid is null)
        {
            throw new PgException(PgSqlStates.InternalError, $"cache lookup failed for relation {ExtensionName}.{table}");
        }

        return reloid.Value;
    }

    /// <summary>
    /// Entry point for this module.
    /// </summary>
    [PgModuleLoad]
    internal static void Load()
    {
        unsafe
        {
            if (!NativeGlobals.process_shared_preload_libraries_in_progress)
            {
                throw new PgException(PgSqlStates.InternalError, "worf is not in shared_preload_libraries");
            }

            if (NativeGlobals.IsBinaryUpgrade)
            {
                return;
            }
        }

        // Init workers.
        Worker.WorkerShmemInit();
    }

    /// <summary>
    /// Selects the directory for dumps: the configured one, or the system's temporary directory.
    /// </summary>
    /// <param name="newval">The configured directory.</param>
    /// <param name="extra">Unused.</param>
    internal static void TempDirectoryAssignHook(string newval, PgGucExtra? extra)
    {
        if (newval.Length != 0)
        {
            s_tempDirectory = newval;
        }
        else if (!OperatingSystem.IsWindows())
        {
            s_tempDirectory = Environment.GetEnvironmentVariable("TMPDIR") ?? "/tmp";
        }
        else
        {
            s_tempDirectory = System.IO.Path.GetTempPath();
        }
    }

    private static unsafe int StrLen(sbyte* value)
    {
        int length = 0;
        while (value[length] != 0)
        {
            length++;
        }

        return length;
    }
}
