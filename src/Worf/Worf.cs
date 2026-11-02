using System.Globalization;
using System.Text;
using Ankus.Postgres;
using Path = System.IO.Path;

namespace Worf;

/// <summary>
/// worf initialization and common functionality.
/// </summary>
internal static partial class Worf
{
    private const uint TextOid = 25;
    private const int AccessShareLock = 1;
    private const ushort BtEqualStrategyNumber = 3;

    /// <summary>
    /// The extension version.
    /// </summary>
    internal const string Version = "0.1.0";

    /// <summary>
    /// The extension version as a number.
    /// </summary>
    internal const int VersionNum = 100;

    /// <summary>
    /// The oldest protocol version this worf speaks.
    /// </summary>
    internal const int MinProtoVersionNum = 1;

    /// <summary>
    /// The newest protocol version this worf speaks.
    /// </summary>
    internal const int MaxProtoVersionNum = 2;

    /// <summary>
    /// The name of the extension, its schema and its library.
    /// </summary>
    internal const string ExtensionName = "worf";

    /// <summary>
    /// The forward origin that forwards changes from every origin.
    /// </summary>
    internal const string ReplicationOriginAll = "all";

    private static readonly Dictionary<uint, Dictionary<short, string>> s_keyEqualityOperators = [];
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
        byte[] hex = Encoding.ASCII.GetBytes(hash.ToString("x8", CultureInfo.InvariantCulture));
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
    /// Cuts a string to the longest start whose bytes in the database encoding fit, at a character's boundary, so that
    /// they stay valid: PostgreSQL checks a SQL_ASCII database's bytes as UTF-8 as it converts them, and can't tell
    /// where its characters end.
    /// </summary>
    /// <param name="value">The string.</param>
    /// <param name="maxBytes">The most bytes the start may take.</param>
    /// <returns>The start.</returns>
    internal static string Truncate(string value, int maxBytes)
    {
        int fits = 0;
        int over = Math.Min(value.Length, maxBytes) + 1;
        while (over - fits > 1)
        {
            int mid = (fits + over) / 2;
            if (ToServer(value[..mid]).Length <= maxBytes)
            {
                fits = mid;
            }
            else
            {
                over = mid;
            }
        }

        if (fits > 0 && fits < value.Length && char.IsLowSurrogate(value[fits]))
        {
            fits--;
        }

        return value[..fits];
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
    /// Widens a transaction id from the last two billion to its full, 64-bit id.
    /// </summary>
    /// <param name="xid">The transaction id.</param>
    /// <returns>The full transaction id.</returns>
    internal static ulong FullTransactionId(uint xid)
    {
        ulong next = NextFullTransactionId;
        ulong epoch = next >> 32;
        return ((xid <= (uint)next || epoch == 0 ? epoch : epoch - 1) << 32) | xid;
    }

    /// <summary>
    /// Gets the full id that the next transaction to need one is given, above every id given so far.
    /// </summary>
    internal static ulong NextFullTransactionId
    {
        get
        {
            unsafe
            {
                return NativeMethods.ReadNextFullTransactionId().value;
            }
        }
    }

    /// <summary>
    /// Gets the full id of the oldest transaction whose commit timestamp and origin PostgreSQL still keeps, or the next
    /// transaction's when it keeps none, as with <c>track_commit_timestamp</c> off. Vacuum moves it on as it truncates
    /// <c>pg_commit_ts</c>, which can be well after the transactions are frozen.
    /// </summary>
    internal static ulong OldestCommitTsFullTransactionId
    {
        get
        {
            uint oldest;
            unsafe
            {
#if ANKUS_PG15 || ANKUS_PG16
                oldest = NativeGlobals.ShmemVariableCache->oldestCommitTsXid;
#else
                oldest = NativeGlobals.TransamVariables->oldestCommitTsXid;
#endif
            }

            return oldest == 0 ? NextFullTransactionId : FullTransactionId(oldest);
        }
    }

    /// <summary>
    /// Gets the full id of this database's <c>datfrozenxid</c>, before which vacuum has frozen every row in its tables.
    /// Vacuum moves it on as it freezes them.
    /// </summary>
    internal static ulong DatabaseFrozenFullTransactionId
        => FullTransactionId((uint)ReadScalar<long>(
            "SELECT datfrozenxid::text::bigint FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()"));

    /// <summary>
    /// Gets the replication origin of this session, <c>replorigin_session_origin</c>.
    /// </summary>
    internal static ushort ReploriginSessionOrigin
    {
        get
        {
            unsafe
            {
#if ANKUS_PG19
                return NativeGlobals.DangerousAddressOf_replorigin_xact_state()->origin;
#else
                return NativeGlobals.replorigin_session_origin;
#endif
            }
        }
    }

    /// <summary>
    /// Gets or sets the remote commit position of the session's transaction, <c>replorigin_session_origin_lsn</c>.
    /// </summary>
    internal static ulong ReploriginSessionOriginLsn
    {
        get
        {
            unsafe
            {
#if ANKUS_PG19
                return NativeGlobals.DangerousAddressOf_replorigin_xact_state()->origin_lsn;
#else
                return NativeGlobals.replorigin_session_origin_lsn;
#endif
            }
        }

        set
        {
            unsafe
            {
#if ANKUS_PG19
                NativeGlobals.DangerousAddressOf_replorigin_xact_state()->origin_lsn = value;
#else
                NativeGlobals.replorigin_session_origin_lsn = value;
#endif
            }
        }
    }

    /// <summary>
    /// Gets or sets the remote commit time of the session's transaction,
    /// <c>replorigin_session_origin_timestamp</c>.
    /// </summary>
    internal static long ReploriginSessionOriginTimestamp
    {
        get
        {
            unsafe
            {
#if ANKUS_PG19
                return NativeGlobals.DangerousAddressOf_replorigin_xact_state()->origin_timestamp;
#else
                return NativeGlobals.replorigin_session_origin_timestamp;
#endif
            }
        }

        set
        {
            unsafe
            {
#if ANKUS_PG19
                NativeGlobals.DangerousAddressOf_replorigin_xact_state()->origin_timestamp = value;
#else
                NativeGlobals.replorigin_session_origin_timestamp = value;
#endif
            }
        }
    }

    /// <summary>
    /// Gets how many bytes of a message are still to be read.
    /// </summary>
    /// <param name="str">The message.</param>
    /// <returns>The number of unread bytes.</returns>
    internal static unsafe int UnreadLength(StringInfoData* str) => str->len - str->cursor;

    /// <summary>
    /// Appends bytes to a StringInfo, whose data argument is <c>char *</c> before PostgreSQL 16.
    /// </summary>
    /// <param name="str">The StringInfo.</param>
    /// <param name="data">The bytes.</param>
    /// <param name="datalen">The number of bytes.</param>
    internal static unsafe void AppendBinaryStringInfo(StringInfoData* str, void* data, int datalen)
    {
#if ANKUS_PG15
        NativeMethods.appendBinaryStringInfo(str, (sbyte*)data, datalen);
#else
        NativeMethods.appendBinaryStringInfo(str, data, datalen);
#endif
    }

    /// <summary>
    /// Sends bytes into a message buffer, whose data argument is <c>char *</c> before PostgreSQL 16.
    /// </summary>
    /// <param name="buf">The message buffer.</param>
    /// <param name="data">The bytes.</param>
    /// <param name="datalen">The number of bytes.</param>
    internal static unsafe void PqSendBytes(StringInfoData* buf, void* data, int datalen)
    {
#if ANKUS_PG15
        NativeMethods.pq_sendbytes(buf, (sbyte*)data, datalen);
#else
        NativeMethods.pq_sendbytes(buf, data, datalen);
#endif
    }

    /// <summary>
    /// Makes a memory context current, as <c>MemoryContextSwitchTo</c> does.
    /// </summary>
    /// <param name="context">The context to switch to.</param>
    /// <returns>The previously current context.</returns>
    internal static unsafe MemoryContextData* MemoryContextSwitchTo(MemoryContextData* context)
    {
        MemoryContextData* old = NativeGlobals.CurrentMemoryContext;
        NativeGlobals.CurrentMemoryContext = context;
        return old;
    }

    /// <summary>
    /// Runs work in a memory context of its own under the current one, deleted as the work ends. In a subtransaction,
    /// that leaves the subtransaction's own context empty, which PostgreSQL then frees as it commits rather than keep it
    /// until the transaction ends, as it would for each of many subtransactions.
    /// </summary>
    /// <param name="work">The work.</param>
    /// <returns>What the work returns, which mustn't point into its context.</returns>
    internal static unsafe T InOwnContext<T>(Func<T> work)
    {
        MemoryContextData* context;
        fixed (byte* name = "worf work\0"u8)
        {
            // The name is static, as a memory context's has to be.
            context = NativeMethods.AllocSetContextCreateInternal(NativeGlobals.CurrentMemoryContext, (sbyte*)name, 0, 8 * 1024, 8 * 1024 * 1024);
        }

        MemoryContextData* oldcontext = MemoryContextSwitchTo(context);
        try
        {
            return work();
        }
        finally
        {
            _ = MemoryContextSwitchTo(oldcontext);
            NativeMethods.MemoryContextDelete(context);
        }
    }

    /// <summary>
    /// Runs work in a memory context of its own under the current one, deleted as the work ends.
    /// </summary>
    /// <param name="work">The work.</param>
    internal static void InOwnContext(Action work) => _ = InOwnContext(() =>
    {
        work();
        return 0;
    });

    /// <summary>
    /// Copies a string into the current memory context in the database encoding, as <c>pstrdup</c> does.
    /// </summary>
    /// <param name="value">The string.</param>
    /// <returns>The terminated copy.</returns>
    internal static unsafe sbyte* PStrDup(string value)
    {
        byte[] bytes = ToServer(value);
        byte* copy = (byte*)NativeMethods.palloc((ulong)bytes.Length + 1);
        bytes.CopyTo(new Span<byte>(copy, bytes.Length));
        copy[bytes.Length] = 0;
        return (sbyte*)copy;
    }

    /// <summary>
    /// Opens one of worf's catalogs without checking the user's access to worf's schema, since worf reads and writes its
    /// catalogs for whoever runs the statement, as from its hooks.
    /// </summary>
    /// <param name="table">The catalog's name.</param>
    /// <param name="lockmode">The lock to take.</param>
    /// <returns>The catalog.</returns>
    internal static unsafe RelationData* OpenCatalog(string table, int lockmode)
    {
        uint relid = CatalogOid(table);
        return relid != 0 ? NativeMethods.table_open(relid, lockmode)
            : throw new PgException(PgSqlStates.UndefinedTable, $"relation \"{ExtensionName}.{table}\" does not exist");
    }

    /// <summary>
    /// Finds one of worf's catalogs.
    /// </summary>
    /// <param name="table">The catalog's name.</param>
    /// <returns>The catalog, or zero when worf isn't installed.</returns>
    internal static unsafe uint CatalogOid(string table)
    {
        uint nspoid = NativeMethods.get_namespace_oid(PStrDup(ExtensionName), true);
        return nspoid == 0 ? 0 : NativeMethods.get_relname_relid(PStrDup(table), nspoid);
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
    internal static unsafe uint GetWorfTableOid(string table)
    {
        uint nspoid;
        uint reloid;
        fixed (byte* extname = ToServerCString(ExtensionName))
        fixed (byte* relname = ToServerCString(table))
        {
            nspoid = NativeMethods.get_namespace_oid((sbyte*)extname, false);

            reloid = NativeMethods.get_relname_relid((sbyte*)relname, nspoid);
        }

        if (reloid == 0)
        {
            throw new PgException(PgSqlStates.InternalError, $"cache lookup failed for relation {ExtensionName}.{table}");
        }

        return reloid;
    }

    /// <summary>
    /// Builds a <c>text[]</c> datum from strings, as <c>strlist_to_textarray</c> does.
    /// </summary>
    /// <param name="list">The strings.</param>
    /// <returns>The array datum.</returns>
    internal static unsafe ulong StrListToTextArray(List<string> list)
    {
        ulong* datums = (ulong*)NativeMethods.palloc((nuint)(sizeof(ulong) * Math.Max(list.Count, 1)));
        for (int i = 0; i < list.Count; i++)
        {
            datums[i] = (ulong)NativeMethods.cstring_to_text(PStrDup(list[i]));
        }

        return (ulong)NativeMethods.construct_array(datums, list.Count, TextOid, -1, false, (sbyte)'i');
    }

    /// <summary>
    /// Reads the strings of a <c>text[]</c> datum, which has no nulls, as <c>textarray_to_list</c> does.
    /// </summary>
    /// <param name="datum">The array datum.</param>
    /// <returns>The strings.</returns>
    internal static unsafe List<string> TextArrayDatumToList(ulong datum)
    {
        ArrayType* array = (ArrayType*)NativeMethods.pg_detoast_datum((varlena*)datum);
        ulong* elems;
        bool* nulls;
        int nelems;
        NativeMethods.deconstruct_array(array, TextOid, -1, false, (sbyte)'i', &elems, &nulls, &nelems);

        List<string> res = [];
        for (int i = 0; i < nelems; i++)
        {
            if (nulls[i])
            {
                throw new PgException(PgSqlStates.NullValueNotAllowed, "null array element not allowed in this context");
            }

            res.Add(FromServer(NativeMethods.text_to_cstring((varlena*)elems[i])));
        }

        return res;
    }

    /// <summary>
    /// Copies a name into a zeroed <c>NameData</c>, truncated at a character boundary, as <c>namein</c> does.
    /// </summary>
    /// <param name="name">The name.</param>
    /// <returns>The <c>NameData</c>.</returns>
    internal static unsafe nint NameIn(sbyte* name)
    {
        byte* result = (byte*)NativeMethods.palloc0(Worker.NameDataLen);
        int len = StrLen(name);
        if (len >= Worker.NameDataLen)
        {
            len = NativeMethods.pg_mbcliplen(name, len, Worker.NameDataLen - 1);
        }

        new ReadOnlySpan<byte>(name, len).CopyTo(new Span<byte>(result, len));
        return (nint)result;
    }

    /// <summary>
    /// Selects rows from our catalog tables with the latest snapshot, as the C's catalog scans without a snapshot do,
    /// so a read after waiting for a lock sees what the lock's holder committed.
    /// </summary>
    /// <param name="commandText">The query.</param>
    /// <param name="parameters">The query's parameters.</param>
    /// <returns>The rows.</returns>
    internal static unsafe SpiResult SelectCatalog(string commandText, params ReadOnlySpan<SpiParameter> parameters)
    {
        int nestLevel = PinSearchPath();
        NativeMethods.PushActiveSnapshot(NativeMethods.GetLatestSnapshot());
        try
        {
            return Spi.Select(commandText, parameters);
        }
        finally
        {
            NativeMethods.PopActiveSnapshot();
            UnpinSearchPath(nestLevel);
        }
    }

    /// <summary>
    /// Selects rows with the snapshot a statement of the transaction takes, as SPI's writable helpers do, without
    /// assigning the transaction an ID as they do even for a SELECT: an idle worker then writes no commit records, and
    /// a read before DROP INDEX CONCURRENTLY leaves it the first action in its transaction.
    /// </summary>
    /// <param name="commandText">The query.</param>
    /// <param name="limit">The most rows to return, or zero for all.</param>
    /// <param name="parameters">The query's parameters.</param>
    /// <returns>The rows.</returns>
    internal static unsafe SpiResult Read(string commandText, int limit, params ReadOnlySpan<SpiParameter> parameters)
    {
        int nestLevel = PinSearchPath();
        NativeMethods.PushActiveSnapshot(NativeMethods.GetTransactionSnapshot());
        try
        {
            return Spi.Select(commandText, limit, parameters);
        }
        finally
        {
            NativeMethods.PopActiveSnapshot();
            UnpinSearchPath(nestLevel);
        }
    }

    /// <summary>
    /// Runs a read-only query of worf's as <see cref="Spi.Select(string, ReadOnlySpan{SpiParameter})"/> does, with the
    /// search path <see cref="PinSearchPath"/> sets.
    /// </summary>
    /// <param name="commandText">The query.</param>
    /// <param name="parameters">The query's parameters.</param>
    /// <returns>The rows.</returns>
    internal static SpiResult Select(string commandText, params ReadOnlySpan<SpiParameter> parameters)
    {
        int nestLevel = PinSearchPath();
        try
        {
            return Spi.Select(commandText, parameters);
        }
        finally
        {
            UnpinSearchPath(nestLevel);
        }
    }

    /// <summary>
    /// Runs a statement of worf's as <see cref="Spi.Execute(string, ReadOnlySpan{SpiParameter})"/> does, with the search
    /// path <see cref="PinSearchPath"/> sets.
    /// </summary>
    /// <param name="commandText">The statement.</param>
    /// <param name="parameters">The statement's parameters.</param>
    /// <returns>The number of rows it processed.</returns>
    internal static long Execute(string commandText, params ReadOnlySpan<SpiParameter> parameters)
    {
        int nestLevel = PinSearchPath();
        try
        {
            return Spi.Execute(commandText, parameters);
        }
        finally
        {
            UnpinSearchPath(nestLevel);
        }
    }

    /// <summary>
    /// Runs a statement of worf's as <see cref="Spi.Execute(SpiCommand)"/> does, with the search path
    /// <see cref="PinSearchPath"/> sets.
    /// </summary>
    /// <param name="command">The statement.</param>
    /// <returns>The number of rows it processed.</returns>
    internal static long Execute(SpiCommand command)
    {
        int nestLevel = PinSearchPath();
        try
        {
            return Spi.Execute(command);
        }
        finally
        {
            UnpinSearchPath(nestLevel);
        }
    }

    /// <summary>
    /// Runs a query of worf's as <see cref="Spi.Query(string, bool, int, ReadOnlySpan{SpiParameter})"/> does, with the
    /// search path <see cref="PinSearchPath"/> sets.
    /// </summary>
    /// <param name="commandText">The query.</param>
    /// <param name="readOnly">Whether it only reads.</param>
    /// <param name="limit">The most rows to return, or zero for all.</param>
    /// <param name="parameters">The query's parameters.</param>
    /// <returns>The rows.</returns>
    internal static SpiResult Query(string commandText, bool readOnly, int limit, params ReadOnlySpan<SpiParameter> parameters)
    {
        int nestLevel = PinSearchPath();
        try
        {
            return Spi.Query(commandText, readOnly, limit, parameters);
        }
        finally
        {
            UnpinSearchPath(nestLevel);
        }
    }

    /// <summary>
    /// Runs a query of worf's as <see cref="Spi.Query(string, ReadOnlySpan{SpiParameter})"/> does, with the search path
    /// <see cref="PinSearchPath"/> sets.
    /// </summary>
    /// <param name="commandText">The query.</param>
    /// <param name="parameters">The query's parameters.</param>
    /// <returns>The rows.</returns>
    internal static SpiResult Query(string commandText, params ReadOnlySpan<SpiParameter> parameters)
    {
        int nestLevel = PinSearchPath();
        try
        {
            return Spi.Query(commandText, parameters);
        }
        finally
        {
            UnpinSearchPath(nestLevel);
        }
    }

    /// <summary>
    /// Runs a query of worf's as <see cref="Spi.Query(SpiCommand, bool, int)"/> does, with the search path
    /// <see cref="PinSearchPath"/> sets.
    /// </summary>
    /// <param name="command">The query.</param>
    /// <param name="readOnly">Whether it only reads.</param>
    /// <param name="limit">The most rows to return, or zero for all.</param>
    /// <returns>The rows.</returns>
    internal static SpiResult Query(SpiCommand command, bool readOnly, int limit)
    {
        int nestLevel = PinSearchPath();
        try
        {
            return Spi.Query(command, readOnly, limit);
        }
        finally
        {
            UnpinSearchPath(nestLevel);
        }
    }

    /// <summary>
    /// Runs a query of worf's as <see cref="Spi.ExecuteScalar{T}(string, ReadOnlySpan{SpiParameter})"/> does, with the
    /// search path <see cref="PinSearchPath"/> sets.
    /// </summary>
    /// <typeparam name="T">The column's managed type.</typeparam>
    /// <param name="commandText">The query.</param>
    /// <param name="parameters">The query's parameters.</param>
    /// <returns>The first column of the first row.</returns>
    internal static T? ExecuteScalar<[PgConvertible] T>(string commandText, params ReadOnlySpan<SpiParameter> parameters)
    {
        int nestLevel = PinSearchPath();
        try
        {
            return Spi.ExecuteScalar<T>(commandText, parameters);
        }
        finally
        {
            UnpinSearchPath(nestLevel);
        }
    }

    /// <summary>
    /// Gets the equality operator of each key column of a unique index, from the column's operator family, written as
    /// <c>OPERATOR(schema.name)</c>, so that SQL comparing the columns that runs under a user's search path, as SQL that
    /// fires their triggers does, uses the index's own operators rather than ones the path finds first.
    /// </summary>
    /// <param name="indexOid">The index.</param>
    /// <returns>Each key column's operator, by its attribute number.</returns>
    internal static unsafe Dictionary<short, string> KeyEqualityOperators(uint indexOid)
    {
        if (s_keyEqualityOperators.TryGetValue(indexOid, out Dictionary<short, string>? cached))
        {
            return cached;
        }

        Dictionary<short, string> operators = [];
        RelationData* index = NativeMethods.index_open(indexOid, AccessShareLock);
        short* indkey = (short*)(&index->rd_index->indkey + 1);
        for (int i = 0; i < index->rd_index->indnkeyatts; i++)
        {
            uint opcintype = index->rd_opcintype[i];
            uint op = NativeMethods.get_opfamily_member(index->rd_opfamily[i], opcintype, opcintype, (short)BtEqualStrategyNumber);
            if (op != 0)
            {
                operators[indkey[i]] = $"OPERATOR({ReadScalar<string>("""
                    SELECT pg_catalog.quote_ident(n.nspname) || '.' || o.oprname
                    FROM pg_catalog.pg_operator o JOIN pg_catalog.pg_namespace n ON n.oid = o.oprnamespace
                    WHERE o.oid = $1
                    """, SpiParameter.Create(op))})";
            }
        }

        NativeMethods.index_close(index, AccessShareLock);
        s_keyEqualityOperators[indexOid] = operators;
        return operators;
    }

    /// <summary>
    /// Runs worf's own work, as a prepared statement's planning and execution, with the search path
    /// <see cref="PinSearchPath"/> sets.
    /// </summary>
    /// <typeparam name="T">The work's result.</typeparam>
    /// <param name="work">The work.</param>
    /// <returns>Its result.</returns>
    internal static T Pinned<T>(Func<T> work)
    {
        int nestLevel = PinSearchPath();
        try
        {
            return work();
        }
        finally
        {
            UnpinSearchPath(nestLevel);
        }
    }

    /// <summary>
    /// Searches the system catalog first and temporary objects last until the GUC nest level it returns ends, so that a
    /// query of worf's, run in a user's session, uses the system's operators and functions rather than ones a user put
    /// ahead of them on the search path.
    /// </summary>
    /// <returns>The nest level, which the caller ends with <see cref="UnpinSearchPath"/>, or zero when the path is
    /// already that, as in worf's workers other than apply and sync.</returns>
    internal static unsafe int PinSearchPath()
    {
        fixed (byte* name = ToServerCString("search_path"))
        {
            if (FromServer(NativeMethods.GetConfigOption((sbyte*)name, false, false)) == PinnedSearchPath)
            {
                return 0;
            }

            int nestLevel = NativeMethods.NewGUCNestLevel();
            fixed (byte* value = ToServerCString(PinnedSearchPath))
            {
                _ = NativeMethods.set_config_option((sbyte*)name, (sbyte*)value, GucContext.PGC_USERSET, GucSource.PGC_S_SESSION,
                    GucAction.GUC_ACTION_SAVE, true, 0, false);
            }

            return nestLevel;
        }
    }

    /// <summary>
    /// Searches the path the database and role set, as <c>RESET search_path</c> would, until the GUC nest level it
    /// returns ends, so that a row filter's functions find the objects they name without a schema as pglogical's do,
    /// on a connection that otherwise searches only the system catalog.
    /// </summary>
    /// <returns>The nest level, which the caller ends with <see cref="UnpinSearchPath"/>.</returns>
    internal static unsafe int DefaultSearchPath()
    {
        fixed (byte* name = ToServerCString("search_path"))
        {
            int nestLevel = NativeMethods.NewGUCNestLevel();
            _ = NativeMethods.set_config_option((sbyte*)name, NativeMethods.GetConfigOptionResetString((sbyte*)name),
                GucContext.PGC_USERSET, GucSource.PGC_S_SESSION, GucAction.GUC_ACTION_SAVE, true, 0, false);
            return nestLevel;
        }
    }

    /// <summary>
    /// The search path worf's own SQL runs with, and its workers other than apply and sync run with.
    /// </summary>
    internal const string PinnedSearchPath = "pg_catalog, pg_temp";

    /// <summary>
    /// Puts back the search path that <see cref="PinSearchPath"/> or <see cref="DefaultSearchPath"/> replaced.
    /// </summary>
    /// <param name="nestLevel">The nest level it returned.</param>
    internal static void UnpinSearchPath(int nestLevel)
    {
        if (nestLevel > 0)
        {
            unsafe
            {
                NativeMethods.AtEOXact_GUC(true, nestLevel);
            }
        }
    }

    /// <summary>
    /// Reads the first column of a query's first row as <see cref="Read"/> reads rows.
    /// </summary>
    /// <typeparam name="T">The column's managed type.</typeparam>
    /// <param name="commandText">The query.</param>
    /// <param name="parameters">The query's parameters.</param>
    /// <returns>The value, or the type's default when the query returns no rows.</returns>
    internal static T ReadScalar<[PgConvertible] T>(string commandText, params ReadOnlySpan<SpiParameter> parameters)
    {
        SpiResult result = Read(commandText, 1, parameters);
        return result.Count == 0 ? default! : result[0].Get<T>(0);
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
        DdlLock.ShmemInit();
        SequenceRanges.ShmemInit();
        SequenceRangeReservations.ShmemInit();
        SequenceRangeHeard.ShmemInit();
        SnowflakeIds.ShmemInit();

        // Init executor module
        Executor.ExecutorInit();

        // Run the supervisor.
        PgBackgroundWorker.Register(new PgBackgroundWorkerOptions("worf supervisor", ExtensionName, "worf_supervisor_main")
        {
            DatabaseAccess = true,
            StartTime = PgBackgroundWorkerStartTime.RecoveryFinished,
            RestartDelay = TimeSpan.FromSeconds(5),
        });
    }

    /// <summary>
    /// Make standard postgres connection, ERROR on failure.
    /// </summary>
    /// <param name="connstring">The connection string.</param>
    /// <param name="connname">The name the connection's application name starts with.</param>
    /// <param name="suffix">The application name's suffix, or null.</param>
    /// <param name="options">Connection options that override the connection string's, or null.</param>
    /// <returns>The connection.</returns>
    internal static PgReplicationConnection Connect(string connstring, string connname, string? suffix, string? options = null)
        => ConnectBase(connstring, connname, suffix, false, options);

    /// <summary>
    /// Make replication connection, ERROR on failure.
    /// </summary>
    /// <param name="connstring">The connection string.</param>
    /// <param name="connname">The name the connection's application name starts with.</param>
    /// <param name="suffix">The application name's suffix, or null.</param>
    /// <returns>The connection.</returns>
    internal static PgReplicationConnection ConnectReplica(string connstring, string connname, string? suffix)
        => ConnectBase(connstring, connname, suffix, true, null);

    /// <summary>
    /// Make sure the extension is up to date. Called by db manager.
    /// </summary>
    internal static void ManageExtension()
    {
        unsafe
        {
            if (NativeMethods.RecoveryInProgress())
            {
                return;
            }

            // make sure we're operating without other worf workers interfering
            NativeMethods.LockRelationOid(Dependency.ExtensionRelationId, WorfNode.ShareUpdateExclusiveLock);
        }

        string? extversion = ReadScalar<string?>(
            "SELECT extversion FROM pg_catalog.pg_extension WHERE extname = $1", SpiParameter.Create(ExtensionName));

        // No extension, nothing to update. Only run the alter if the versions don't match.
        if (extversion is not null && extversion != Version)
        {
            Execute("ALTER EXTENSION " + Spi.QuoteIdentifier(ExtensionName) + " UPDATE");
        }
    }

    /// <summary>
    /// Call IDENTIFY_SYSTEM on the connection and report its results.
    /// </summary>
    /// <param name="streamConn">The replication connection.</param>
    /// <returns>The system identifier, timeline, WAL position and database name.</returns>
    internal static (ulong Sysid, uint Timeline, ulong Xlogpos, string Dbname) IdentifySystem(PgReplicationConnection streamConn)
    {
        IReadOnlyList<string?[]> res;
        try
        {
            res = streamConn.Query("IDENTIFY_SYSTEM", 4);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError,
                $"could not send replication command \"IDENTIFY_SYSTEM\": ERROR:  {error.Message}\n");
        }

        if (res.Count != 1)
        {
            throw new PgException(PgSqlStates.InternalError,
                $"could not identify system: got {res.Count} rows and 4 fields, expected 1 rows and at least 4 fields\n");
        }

        string?[] row = res[0];
        if (!ulong.TryParse(row[0], NumberStyles.None, CultureInfo.InvariantCulture, out ulong sysid))
        {
            throw new PgException(PgSqlStates.InternalError, $"could not parse remote sysid {row[0]}");
        }

        if (!uint.TryParse(row[1], NumberStyles.None, CultureInfo.InvariantCulture, out uint timeline))
        {
            throw new PgException(PgSqlStates.InternalError, $"could not parse remote tlid {row[1]}");
        }

        if (row[2] is null || !TryParseLsn(row[2]!, out ulong xlogpos))
        {
            throw new PgException(PgSqlStates.InternalError, $"could not parse remote xlogpos {row[2]}");
        }

        return (sysid, timeline, xlogpos, row[3] ?? "");
    }

    /// <summary>
    /// Starts streaming from a replication slot with worf's output plugin.
    /// </summary>
    /// <param name="streamConn">The replication connection.</param>
    /// <param name="slotName">The slot.</param>
    /// <param name="startPos">The position to stream from.</param>
    /// <param name="forwardOrigins">The forwarded origins, or null.</param>
    /// <param name="replicationSets">The replication sets, or null.</param>
    /// <param name="replicateOnlyTable">The only table to replicate, or null.</param>
    /// <param name="forceTextTransfer">Whether to transfer data as text.</param>
    internal static void StartReplication(PgReplicationConnection streamConn, string slotName, ulong startPos,
        string? forwardOrigins, string? replicationSets, string? replicateOnlyTable, bool forceTextTransfer)
    {
        string wantBinary = forceTextTransfer ? "0" : "1";
        int pgVersionNum = ServerVersionNum;
        StringBuilder command = new();
        command.Append(CultureInfo.InvariantCulture,
            $"START_REPLICATION SLOT \"{slotName}\" LOGICAL {startPos >> 32:X}/{(uint)startPos:X} (");

        // Basic protocol info.
        command.Append(CultureInfo.InvariantCulture, $"expected_encoding '{DatabaseEncodingName}'");
        command.Append(CultureInfo.InvariantCulture, $", min_proto_version '{MinProtoVersionNum}'");
        command.Append(CultureInfo.InvariantCulture, $", max_proto_version '{MaxProtoVersionNum}'");
        command.Append(", startup_params_format '1'");

        // Binary protocol compatibility. Values in PostgreSQL's internal format can't take the writer's replicated rows,
        // which read text and binary, so they would apply more slowly than binary.
        command.Append(", \"binary.want_internal_basetypes\" '0'");
        command.Append(CultureInfo.InvariantCulture, $", \"binary.want_binary_basetypes\" '{wantBinary}'");
        command.Append(CultureInfo.InvariantCulture, $", \"binary.basetypes_major_version\" '{pgVersionNum / 100}'");
        command.Append(CultureInfo.InvariantCulture, $", \"binary.sizeof_datum\" '{SizeofDatum}'");
        command.Append(CultureInfo.InvariantCulture, $", \"binary.sizeof_int\" '{SizeofInt}'");
        command.Append(CultureInfo.InvariantCulture, $", \"binary.sizeof_long\" '{SizeofLong}'");
        command.Append(CultureInfo.InvariantCulture, $", \"binary.bigendian\" '{(BigEndian ? 1 : 0)}'");
        command.Append(CultureInfo.InvariantCulture, $", \"binary.float4_byval\" '{(Float4Byval ? 1 : 0)}'");
        command.Append(CultureInfo.InvariantCulture, $", \"binary.float8_byval\" '{(Float8Byval ? 1 : 0)}'");
        command.Append(CultureInfo.InvariantCulture, $", \"binary.integer_datetimes\" '{(IntegerDatetimes ? 1 : 0)}'");

        // We don't care about this anymore but older output plugins expect this.
        command.Append(", \"hooks.setup_function\" 'worf.worf_hooks_setup'");

        if (forwardOrigins is not null)
        {
            command.Append(", \"worf.forward_origins\" ").Append(Spi.QuoteLiteral(forwardOrigins));
        }

        if (replicateOnlyTable is not null)
        {
            // Send the table name we want to the upstream
            command.Append(", \"worf.replicate_only_table\" ").Append(Spi.QuoteLiteral(replicateOnlyTable));
        }

        if (replicationSets is not null)
        {
            // Send the replication set names we want to the upstream
            command.Append(", \"worf.replication_set_names\" ").Append(Spi.QuoteLiteral(replicationSets));
        }

        // Tell the upstream that we want unbounded metadata cache size
        command.Append(", \"relmeta_cache_size\" '-1'");

        // general info about the downstream
        command.Append(CultureInfo.InvariantCulture, $", pg_version '{pgVersionNum}'");
        command.Append(CultureInfo.InvariantCulture, $", worf_version '{Version}'");
        command.Append(CultureInfo.InvariantCulture, $", worf_version_num '{VersionNum}'");
        command.Append(CultureInfo.InvariantCulture, $", worf_apply_pid '{Environment.ProcessId}'");

        command.Append(')');

        try
        {
            streamConn.StartCopy(command.ToString());
        }
        catch (PgRemoteException error)
        {
            PgLog.Write(PgLogLevel.Fatal, new PgDiagnostic(
                $"could not send replication command \"{command}\": ERROR:  {error.Message}\n, sqlstate: {error.SqlState}"));
        }
    }

    /// <summary>
    /// Gets the server's version number, as <c>PG_VERSION_NUM</c> holds it.
    /// </summary>
    internal static int ServerVersionNum
        => int.Parse(GetConfigOptionByName("server_version_num")!, CultureInfo.InvariantCulture);

    /// <summary>
    /// Gets the database encoding's name.
    /// </summary>
    internal static unsafe string DatabaseEncodingName => FromServer(NativeMethods.GetDatabaseEncodingName());

    /// <summary>
    /// Gets <c>sizeof(Datum)</c>.
    /// </summary>
    internal static int SizeofDatum => 8;

    /// <summary>
    /// Gets <c>sizeof(int)</c>.
    /// </summary>
    internal static int SizeofInt => 4;

    /// <summary>
    /// Gets <c>sizeof(long)</c>, which is 4 on Windows and 8 elsewhere.
    /// </summary>
    internal static int SizeofLong => OperatingSystem.IsWindows() ? 4 : 8;

    /// <summary>
    /// Gets whether the server is big-endian (<c>WORDS_BIGENDIAN</c>).
    /// </summary>
    internal static bool BigEndian => !BitConverter.IsLittleEndian;

    /// <summary>
    /// Gets whether <c>USE_FLOAT4_BYVAL</c> is defined, which no supported PostgreSQL release does.
    /// </summary>
    internal static bool Float4Byval => false;

    /// <summary>
    /// Gets whether <c>USE_FLOAT8_BYVAL</c> is defined, as it is on every 64-bit server.
    /// </summary>
    internal static bool Float8Byval => true;

    /// <summary>
    /// Gets whether <c>USE_INTEGER_DATETIMES</c> is defined, which PostgreSQL 10 and later never do.
    /// </summary>
    internal static bool IntegerDatetimes => false;

    /// <summary>
    /// Parses a WAL position in its <c>%X/%X</c> text form.
    /// </summary>
    /// <param name="text">The text.</param>
    /// <param name="lsn">The position.</param>
    /// <returns>Whether the text parsed.</returns>
    internal static bool TryParseLsn(string text, out ulong lsn)
    {
        lsn = 0;
        int slash = text.IndexOf('/', StringComparison.Ordinal);
        if (slash <= 0 ||
            !uint.TryParse(text.AsSpan(0, slash), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out uint high) ||
            !uint.TryParse(text.AsSpan(slash + 1), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out uint low))
        {
            return false;
        }

        lsn = ((ulong)high << 32) | low;
        return true;
    }

    /// <summary>
    /// Creates an SPI parameter of type <c>pg_lsn</c>.
    /// </summary>
    /// <param name="lsn">The position.</param>
    /// <returns>The parameter.</returns>
    internal static SpiParameter LsnParameter(ulong lsn)
    {
        const uint LsnOid = 3220;
        unsafe
        {
            return SpiParameter.Create(PgDatum.DangerousCreate((nuint)lsn, LsnOid, PgMemoryContext.Current));
        }
    }

    /// <summary>
    /// Formats a WAL position as <c>%X/%X</c>.
    /// </summary>
    /// <param name="lsn">The position.</param>
    /// <returns>The text.</returns>
    internal static string FormatLsn(ulong lsn)
        => string.Create(CultureInfo.InvariantCulture, $"{lsn >> 32:X}/{(uint)lsn:X}");

    /// <summary>
    /// Static bgworker used for initialization and management (our main process).
    /// </summary>
    /// <param name="mainArg">Unused.</param>
    [PgBackgroundWorker(EntryPoint = "worf_supervisor_main")]
    internal static void SupervisorMain(nuint mainArg)
    {
        _ = mainArg;

        // Initialize supervisor info in shared memory. Strictly speaking we don't need a lock here, because no other
        // process could possibly be looking at this shared struct since they're all started by the supervisor, but
        // let's be safe.
        using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
        {
            WorfContext ctx = guard.Header;
            unsafe
            {
                ctx.Supervisor = (nint)NativeGlobals.MyProc;
            }

            ctx.SubscriptionsChanged = true;
            guard.Header = ctx;
        }

        // Make it easy to identify our processes.
        Worker.SetConfigOption("application_name", PgBackgroundWorker.Name);

        PgLog.Log("starting worf supervisor");

        // Setup connection to pinned catalogs (we only ever read pg_database).
        PgBackgroundWorker.Connect(null, null);

        // Main wait loop.
        while (!Worker.GotSigterm)
        {
            PgInterrupts.Check();
            Worker.ProcessConfigReload();

            bool subscriptionsChanged;
            using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
            {
                // We'll take account of all sub changes up to this point, even if new ones were added between the
                // test and the flag clear. We're just being woken up.
                WorfContext ctx = guard.Header;
                subscriptionsChanged = ctx.SubscriptionsChanged;
                ctx.SubscriptionsChanged = false;
                guard.Header = ctx;
            }

            if (subscriptionsChanged)
            {
                try
                {
                    PgBackgroundWorker.RunTransaction(StartManagerWorkers);
                }
                catch (OperationCanceledException) when (Worker.GotSigterm)
                {
                    return;
                }
            }

            // Wait for latch signal or timeout. On Windows, use a shorter timeout to ensure faster response to
            // subscription changes, as process startup and IPC can be slower than on Unix systems.
            if (!PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(OperatingSystem.IsWindows() ? 30000 : 180000)))
            {
                break;
            }
        }
    }

    /// <summary>
    /// Start the manager workers for every db which has a worf node. Note that we start workers that are not
    /// necessary here. We do this because we need to check every individual database to check if there is worf
    /// node setup and it's not possible to switch connections to different databases within one background
    /// worker. The workers that won't find any worf node setup will exit immediately during startup. Must be run
    /// inside a transaction.
    /// </summary>
    private static unsafe void StartManagerWorkers()
    {
        const uint DatabaseRelationId = 1262;

        // Run manager worker for every connectable database.
        RelationData* rel = NativeMethods.table_open(DatabaseRelationId, (int)PgLockMode.AccessShare);
        TableScanDescData* scan = NativeMethods.table_beginscan_catalog(rel, 0, null);

        HeapTupleData* tup;
        while ((tup = NativeMethods.heap_getnext(scan, ScanDirection.ForwardScanDirection)) is not null)
        {
            FormData_pg_database* pgdatabase = (FormData_pg_database*)NativeMethods.GETSTRUCT(tup);
            uint dboid = pgdatabase->oid;

            PgInterrupts.Check();

            // Can't run workers on databases which don't allow connection.
            if (!pgdatabase->datallowconn)
            {
                continue;
            }

            // Worker already attached, nothing to do.
            using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
            {
                if (guard.Read((in _, workers) =>
                    Worker.WorkerRunning(workers, Worker.ManagerFind(workers, dboid))))
                {
                    continue;
                }
            }

            // No record found, try running new worker.
            PgLog.Debug1($"registering worf manager process for database {FromServer((sbyte*)&pgdatabase->datname)}");

            WorfWorker worker = default;
            worker.WorkerType = WorfWorkerType.Manager;
            worker.Dboid = dboid;

            _ = Worker.WorkerRegister(worker);
        }

        NativeMethods.table_endscan(scan);
        NativeMethods.table_close(rel, (int)PgLockMode.AccessShare);
    }

    /// <summary>
    /// Gets the application name a connection made with <see cref="Connect"/> or <see cref="ConnectReplica"/> has.
    /// </summary>
    /// <param name="connname">The name the connection's application name starts with.</param>
    /// <param name="suffix">The application name's suffix, or null.</param>
    /// <returns>The application name.</returns>
    internal static string ApplicationName(string connname, string? suffix)
    {
        if (suffix is null)
        {
            return connname;
        }

        string applicationName = $"{ShortenHash(connname, Worker.NameDataLen - ToServer(suffix).Length - 2)}_{suffix}";
        byte[] bytes = ToServer(applicationName);
        return bytes.Length > Worker.NameDataLen - 1 ? Truncate(applicationName, Worker.NameDataLen - 1) : applicationName;
    }

    private static PgReplicationConnection ConnectBase(string connstr, string appname, string? suffix, bool replication, string? options)
    {
        string s = ExtraConnectionOptions + " " + connstr;
        string applicationName = ApplicationName(appname, suffix);

        // We use the connection string as the base and pass some extra options after it, which override the ones it
        // names, as libpq's expand_dbname does.
        StringBuilder conninfo = new(connstr);
        conninfo.Append(" application_name=");
        Fe.AppendConnstrValue(conninfo, applicationName);
        conninfo.Append(" connect_timeout=30 keepalives=1 keepalives_idle=20 keepalives_interval=20 keepalives_count=5");
        if (options is not null)
        {
            conninfo.Append(' ').Append(options);
        }

        bool inTransaction;
        unsafe
        {
            inTransaction = NativeMethods.IsTransactionState();
        }

        if (!inTransaction)
        {
            return WithPinnedSearchPath(PgReplicationConnection.Connect(conninfo.ToString(), applicationName, replication));
        }

        PgReplicationConnection? conn = null;
        try
        {
            PgTransaction.RunInSubtransaction(() => conn = PgReplicationConnection.Connect(conninfo.ToString(), applicationName, replication));
        }
        catch (PgException error) when (error.SqlState == PgSqlStates.ConnectionFailure)
        {
            const string prefix = "could not connect for replication: ";
            string failure = error.Message.StartsWith(prefix, StringComparison.Ordinal) ? error.Message[prefix.Length..] : error.Message;

            // libpqwalreceiver chomps the newline that PQerrorMessage ends with.
            if (!failure.EndsWith('\n'))
            {
                failure += "\n";
            }

            throw new PgException(PgSqlStates.InternalError,
                $"could not connect to the postgresql server{(replication ? " in replication mode" : "")}: {failure}",
                detail: $"dsn was: {s}");
        }

        return WithPinnedSearchPath(conn!);
    }

    /// <summary>
    /// Has the session search only the system catalog and then temporary objects, so that worf's queries to the other
    /// node use the system's operators and functions rather than ones its database or role put ahead of them, whose
    /// owner could have them run as the connection's user. Row filters run under the path the database and role set,
    /// as pglogical's do, through <see cref="DefaultSearchPath"/>.
    /// </summary>
    private static PgReplicationConnection WithPinnedSearchPath(PgReplicationConnection conn)
    {
        conn.Execute($"SET search_path = {PinnedSearchPath}");
        return conn;
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
            s_tempDirectory = Path.GetTempPath();
        }
    }

    internal static unsafe int StrLen(sbyte* value)
    {
        int length = 0;
        while (value[length] != 0)
        {
            length++;
        }

        return length;
    }
}
