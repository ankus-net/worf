// worf_create_subscriber turns a physical standby, or a new base backup, of a provider into a worf subscriber.

using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Npgsql;
using Worf;
using Worf.CreateSubscriber;

const int MaxApplyDelay = 86400;
const int NameDataLen = 64;
const string LogFileName = "worf_create_subscriber_postgres.log";
string argv0 = Environment.ProcessPath ?? Environment.GetCommandLineArgs()[0];
string progname = Path.GetFileNameWithoutExtension(argv0);
string? dataDir = null;
string pidFile = "";
VerbosityLevel verbosity = VerbosityLevel.Normal;

// Die closes these.
NpgsqlConnection? subscriberConn = null;
NpgsqlConnection? providerConn = null;

string? remoteLsn;
bool stop = false;
bool dropSlotIfExists = false;
string? subscriberName = null;
string? baseSubConnstr = null;
string? baseProvConnstr = null;
string? replicationSets = null;
string? databases = null;
string? postgresqlConf = null;
string? pgHbaConf = null;
string? recoveryConf = null;
int applyDelay = 0;
bool forceTextTransfer = false;
string[] databaseList;
bool useExistingDataDir = false;
int pgCtlRet;
string? extraBasebackupArgs = null;
StringBuilder recoveryconfcontents = new();

Console.CancelKeyPress += (_, _) => _ = Die("\nCanceling...\n");

// check for --help
foreach (string arg in args)
{
    if (arg is "--help" or "-?")
    {
        Usage();
        return 0;
    }
}

// Option parsing and validation
for (int i = 0; i < args.Length; i++)
{
    string arg = args[i];
    if (!TryParseOption(args, ref i, out string option, out string? value))
    {
        Console.Error.WriteLine($"{progname}: unrecognized option '{arg}'");
        Console.Error.WriteLine("Unknown option");
        Console.Error.WriteLine($"Try \"{progname} --help\" for more information.");
        return 1;
    }

    switch (option)
    {
        case "D":
            dataDir = value;
            break;
        case "n":
            subscriberName = value;
            break;
        case "provider-dsn":
            baseProvConnstr = value;
            break;
        case "subscriber-dsn":
            baseSubConnstr = value;
            break;
        case "replication-sets":
            replicationSets = ValidateReplicationSetInput(value!);
            break;
        case "postgresql-conf":
            postgresqlConf = value;
            if (!FileExists(postgresqlConf!))
            {
                throw Die("The specified postgresql.conf file does not exist.");
            }

            break;
        case "hba-conf":
            pgHbaConf = value;
            if (!FileExists(pgHbaConf!))
            {
                throw Die("The specified pg_hba.conf file does not exist.");
            }

            break;
        case "recovery-conf":
            recoveryConf = value;
            if (!FileExists(recoveryConf!))
            {
                throw Die("The specified recovery configuration file does not exist.");
            }

            break;
        case "v":
            verbosity++;
            break;
        case "s":
            stop = true;
            break;
        case "drop-slot-if-exists":
            dropSlotIfExists = true;
            break;
        case "apply-delay":
            applyDelay = Atoi(value!);
            break;
        case "databases":
            databases = value;
            break;
        case "extra-basebackup-args":
            extraBasebackupArgs = value;
            break;
        case "text-types":
            forceTextTransfer = true;
            break;
        default:
            Console.Error.WriteLine("Unknown option");
            Console.Error.WriteLine($"Try \"{progname} --help\" for more information.");
            return 1;
    }
}

// Sanity checks
if (dataDir is null)
{
    Console.Error.WriteLine("No data directory specified");
    Console.Error.WriteLine($"Try \"{progname} --help\" for more information.");
    return 1;
}
else if (subscriberName is null)
{
    Console.Error.WriteLine("No subscriber name specified");
    Console.Error.WriteLine($"Try \"{progname} --help\" for more information.");
    return 1;
}

if (string.IsNullOrEmpty(baseProvConnstr))
{
    throw Die("Provider connection string must be specified.\n");
}

if (string.IsNullOrEmpty(baseSubConnstr))
{
    throw Die("Subscriber connection string must be specified.\n");
}

if (applyDelay < 0)
{
    throw Die("Apply delay cannot be negative.\n");
}

if (applyDelay > MaxApplyDelay)
{
    throw Die($"Apply delay cannot be more than {MaxApplyDelay}.\n");
}

if (string.IsNullOrEmpty(replicationSets))
{
    replicationSets = "default,default_insert_only,ddl_sql";
}

// Parse database list or connection string.
if (databases is not null)
{
    databaseList = GetDatabaseList(databases);
}
else
{
    string dbname = GetConnstrDbname(baseProvConnstr)
        ?? throw Die("Either provider connection string must contain database name or --databases option must be specified.\n");

    databaseList = [dbname];
}

string[] slotNames = new string[databaseList.Length];

// Check connection strings for validity before doing anything expensive.
foreach (string db in databaseList)
{
    if (string.IsNullOrEmpty(GetConnstr(baseProvConnstr, db)))
    {
        throw Die("Provider connection string is not valid.\n");
    }

    if (string.IsNullOrEmpty(GetConnstr(baseSubConnstr, db)))
    {
        throw Die("Subscriber connection string is not valid.\n");
    }
}

// Create log file where new postgres instance will log to while being initialized.
try
{
    using FileStream logfd = new(LogFileName, FileMode.OpenOrCreate, FileAccess.ReadWrite);
}
catch (Exception error) when (error is IOException or UnauthorizedAccessException)
{
    throw Die($"Creating {LogFileName} failed: {error.Message}");
}

// Let's start the real work...
PrintMsg(VerbosityLevel.Normal, $"{progname}: starting ...\n");

for (int dbnum = 0; dbnum < databaseList.Length; dbnum++)
{
    string db = databaseList[dbnum];

    string provConnstr = GetConnstr(baseProvConnstr, db);
    if (string.IsNullOrEmpty(provConnstr))
    {
        throw Die("Provider connection string is not valid.\n");
    }

    // Read the remote server indetification.
    PrintMsg(VerbosityLevel.Normal, $"Getting information for database {db} ...\n");
    providerConn = Connectdb(provConnstr);
    RemoteInfo remoteInfo = GetRemoteInfo(providerConn);

    // only need to do this piece once
    if (dbnum == 0)
    {
        useExistingDataDir = CheckDataDir(dataDir);

        if (useExistingDataDir && remoteInfo.Sysid != ReadSysid(dataDir))
        {
            throw Die("Subscriber data directory is not basebackup of remote node.\n");
        }
    }

    // Create replication slots on remote node.
    PrintMsg(VerbosityLevel.Normal, $"Creating replication slot in database {db} ...\n");
    slotNames[dbnum] = InitializeReplicationSlot(providerConn, remoteInfo.Dbname, remoteInfo.NodeName,
        subscriberName, dropSlotIfExists);
    providerConn.Dispose();
    providerConn = null;
}

// Create basebackup or use existing one
string prov = GetConnstr(baseProvConnstr, databaseList[0]);
string sub = GetConnstr(baseSubConnstr, databaseList[0]);

InitializeDataDir(dataDir, useExistingDataDir ? null : prov, postgresqlConf, pgHbaConf, extraBasebackupArgs);
pidFile = Path.Join(dataDir, "postmaster.pid");

string restorePointName = GenerateRestorePointName();

PrintMsg(VerbosityLevel.Normal, $"Creating restore point \"{restorePointName}\" on remote node ...\n");
providerConn = Connectdb(prov);
remoteLsn = CreateRestorePoint(providerConn, restorePointName);
providerConn.Dispose();
providerConn = null;

// Get subscriber db to consistent state (for lsn after slot creation).
PrintMsg(VerbosityLevel.Normal, "Bringing subscriber node to the restore point ...\n");
if (recoveryConf is not null)
{
    CopyConfFile(recoveryConf, "postgresql.auto.conf", true);
}
else
{
    recoveryconfcontents.Append(CultureInfo.InvariantCulture, $"primary_conninfo = '{EscapeSingleQuotesAscii(prov)}'\n");
}

recoveryconfcontents.Append(CultureInfo.InvariantCulture, $"recovery_target_name = '{restorePointName}'\n");
recoveryconfcontents.Append("recovery_target_inclusive = true\n");
recoveryconfcontents.Append("recovery_target_action = promote\n");
WriteRecoveryConf(recoveryconfcontents.ToString());

// Start subscriber node with worf disabled, and wait until it starts accepting connections which means it has
// caught up to the restore point.
// pg_ctl passes the options to the server through a shell on Unix, which turns '' into an empty value, and directly
// on Windows, which keeps the quotes, so the empty value is written as nothing after the equals sign.
pgCtlRet = RunPgCtl(["start", "-l", LogFileName, "-o", "-c shared_preload_libraries="]);
if (pgCtlRet != 0)
{
    throw Die($"Postgres startup for restore point catchup failed with {pgCtlRet}. See {LogFileName}.");
}

WaitPrimaryConnection(sub);

// Clean any per-node data that were copied by pg_basebackup.
PrintMsg(VerbosityLevel.Verbose, "Removing old worf configuration ...\n");

foreach (string subConnstr in databaseList.Select(db => GetConnstr(baseSubConnstr, db)))
{
    if (string.IsNullOrEmpty(subConnstr))
    {
        throw Die("Subscriber connection string is not valid.\n");
    }

    subscriberConn = Connectdb(subConnstr);
    RemoveUnwantedData(subscriberConn);
    subscriberConn.Dispose();
    subscriberConn = null;
}

// Stop Postgres so we can reset system id and start it with worf loaded.
pgCtlRet = RunPgCtl(["stop"]);
if (pgCtlRet != 0)
{
    throw Die($"Postgres stop after restore point catchup failed with {pgCtlRet}. See {LogFileName}.");
}

WaitPostmasterShutdown();

// Start the node again, now with worf active so that we can start the logical replication. This is final
// start, so don't log to to special log file anymore.
PrintMsg(VerbosityLevel.Normal, "Initializing worf on the subscriber node:\n");

pgCtlRet = RunPgCtl(["start"]);
if (pgCtlRet != 0)
{
    throw Die($"Postgres restart with worf enabled failed with {pgCtlRet}.");
}

WaitPostmasterConnection(baseSubConnstr);

for (int dbnum = 0; dbnum < databaseList.Length; dbnum++)
{
    string db = databaseList[dbnum];

    string subConnstr = GetConnstr(baseSubConnstr, db);
    string provConnstr = GetConnstr(baseProvConnstr, db);

    subscriberConn = Connectdb(subConnstr);

    // Create the extension.
    PrintMsg(VerbosityLevel.Verbose, $"Creating worf extension for database {db}...\n");
    InstallExtension(subscriberConn, "worf");

    // Create the identifier which is setup with the position to which we already caught up using physical
    // replication.
    PrintMsg(VerbosityLevel.Verbose, $"Creating replication origin for database {db}...\n");
    InitializeReplicationOrigin(subscriberConn, slotNames[dbnum], remoteLsn);

    // And finally add the node to the cluster.
    PrintMsg(VerbosityLevel.Normal, $"Creating subscriber {subscriberName} for database {db}...\n");
    PrintMsg(VerbosityLevel.Verbose, $"Replication sets: {replicationSets}\n");

    WorfSubscribe(subscriberConn, subscriberName, subConnstr, provConnstr, replicationSets, applyDelay,
        forceTextTransfer);

    subscriberConn.Dispose();
    subscriberConn = null;
}

// If user does not want the node to be running at the end, stop it.
if (stop)
{
    PrintMsg(VerbosityLevel.Normal, "Stopping the subscriber node ...\n");
    pgCtlRet = RunPgCtl(["stop"]);
    if (pgCtlRet != 0)
    {
        throw Die($"Stopping postgres after successful subscribtion failed with {pgCtlRet}.");
    }

    WaitPostmasterShutdown();
}

PrintMsg(VerbosityLevel.Normal, "All done\n");

return 0;

// Print help.
void Usage()
{
    Console.Write($"""
        {progname} create new worf subscriber from basebackup of provider.

        Usage:
          {progname} [OPTION]...

        General options:
          -D, --pgdata=DIRECTORY      data directory to be used for new node,
                                      can be either empty/non-existing directory,
                                      or directory populated using
                                      pg_basebackup -X stream command
          --databases                 optional list of databases to replicate
          -n, --subscriber-name=NAME  name of the newly created subscriber
          --subscriber-dsn=CONNSTR    connection string to the newly created subscriber
          --provider-dsn=CONNSTR      connection string to the provider
          --replication-sets=SETS     comma separated list of replication set names
          --apply-delay=DELAY         apply delay in seconds (by default 0)
          --drop-slot-if-exists       drop replication slot of conflicting name
          -s, --stop                  stop the server once the initialization is done
          -v                          increase logging verbosity
          --extra-basebackup-args     additional arguments to pass to pg_basebackup.
                                      Safe options: -T, -c, --xlogdir/--waldir

        Configuration files override:
          --hba-conf              path to the new pg_hba.conf
          --postgresql-conf       path to the new postgresql.conf
          --recovery-conf         path to the template recovery configuration

        """);
}

// Reads one option, as getopt_long does with the short options D:n:sv and the long options.
// Returns whether the argument is a known option with its value.
bool TryParseOption(string[] args, ref int index, out string option, out string? value)
{
    string arg = args[index];
    value = null;

    if (arg.StartsWith("--", StringComparison.Ordinal))
    {
        string name = arg[2..];
        int equals = name.IndexOf('=', StringComparison.Ordinal);
        if (equals >= 0)
        {
            value = name[(equals + 1)..];
            name = name[..equals];
        }

        option = name switch
        {
            "subscriber-name" => "n",
            "pgdata" => "D",
            "stop" => "s",
            _ => name,
        };

        bool takesValue = option is "n" or "D" or "provider-dsn" or "subscriber-dsn" or "replication-sets" or
            "postgresql-conf" or "hba-conf" or "recovery-conf" or "apply-delay" or "databases" or "extra-basebackup-args";
        bool isFlag = option is "s" or "drop-slot-if-exists" or "text-types";
        if (takesValue)
        {
            if (value is null)
            {
                if (index + 1 >= args.Length)
                {
                    return false;
                }

                value = args[++index];
            }

            return true;
        }

        return isFlag && value is null;
    }

    if (arg.Length >= 2 && arg[0] == '-')
    {
        option = arg[1].ToString();
        switch (arg[1])
        {
            case 'D':
            case 'n':
                if (arg.Length > 2)
                {
                    value = arg[2..];
                }
                else if (index + 1 < args.Length)
                {
                    value = args[++index];
                }
                else
                {
                    return false;
                }

                return true;

            case 's':
            case 'v':
                // Grouped flags, such as -vv, repeat the option.
                for (int c = 2; c < arg.Length; c++)
                {
                    if (arg[c] == 'v')
                    {
                        verbosity++;
                    }
                    else if (arg[c] == 's')
                    {
                        return false;
                    }
                    else
                    {
                        return false;
                    }
                }

                return true;

            default:
                return false;
        }
    }

    option = "";
    return false;
}

// Print error and exit.
// Returns nothing; the process exits. Callers throw the result so the compiler knows execution stops.
InvalidOperationException Die(string message)
{
    Console.Error.Write(message);

    subscriberConn?.Dispose();
    providerConn?.Dispose();

    if (GetPgpid() != 0 && RunPgCtl(["stop", "-s"]) != 0)
    {
        Console.Error.WriteLine("WARNING: postgres seems to be running, but could not be stopped");
    }

    Environment.Exit(1);
    return new InvalidOperationException(message);
}

// Print message to stdout and flush.
void PrintMsg(VerbosityLevel level, string message)
{
    if (verbosity >= level)
    {
        Console.Out.Write(message);
        Console.Out.Flush();
    }
}

NpgsqlConnection Connectdb(string connstr)
{
    try
    {
        return Libpq.Connect(connstr);
    }
    catch (Exception error) when (error is NpgsqlException or ArgumentException)
    {
        throw Die($"Connection to database failed: {error.Message}, connection string was: {connstr}\n");
    }
}

// Start pg_ctl with given argument(s) - used to start/stop postgres.
// Returns the exit code reported by pg_ctl.
int RunPgCtl(IReadOnlyList<string> arg)
{
    string execPath = FindOtherExecOrDie(argv0, "pg_ctl");
    List<string> arguments = [.. arg, "-D", dataDir!];

    // Run pg_ctl in silent mode unless we run in debug mode.
    if (verbosity < VerbosityLevel.Debug && !arguments.Contains("-s"))
    {
        arguments.Add("-s");
    }

    PrintMsg(VerbosityLevel.Debug, $"Running pg_ctl: {FormatCommand(execPath, arguments)}.\n");
    return RunProgram(execPath, arguments);
}

// Run pg_basebackup to create the copy of the origin node.
void RunBasebackup(string providerConnstr, string dataDir, string? extraBasebackupArgs)
{
    string execPath = FindOtherExecOrDie(argv0, "pg_basebackup");
    List<string> arguments = ["-D", dataDir, "-d", providerConnstr, "-X", "s", "-P"];

    // Run pg_basebackup in verbose mode if we are running in verbose mode.
    if (verbosity >= VerbosityLevel.Verbose)
    {
        arguments.Add("-v");
    }

    if (extraBasebackupArgs is not null)
    {
        arguments.AddRange(SplitArguments(extraBasebackupArgs));
    }

    PrintMsg(VerbosityLevel.Debug, $"Running pg_basebackup: {FormatCommand(execPath, arguments)}.\n");
    int ret = RunProgram(execPath, arguments);

    if (ret != 0)
    {
        throw Die($"pg_basebackup failed with exit status {ret}, cannot continue.\n");
    }
}

// Init the datadir.
// This function can either ensure provided datadir is a postgres datadir, or create it using pg_basebackup. In
// any case, new postresql.conf and pg_hba.conf will be copied to the datadir if they are provided.
void InitializeDataDir(string dataDir, string? connstr, string? postgresqlConf, string? pgHbaConf,
    string? extraBasebackupArgs)
{
    if (connstr is not null)
    {
        PrintMsg(VerbosityLevel.Normal, "Creating base backup of the remote node...\n");
        RunBasebackup(connstr, dataDir, extraBasebackupArgs);
    }

    if (postgresqlConf is not null)
    {
        CopyConfFile(postgresqlConf, "postgresql.conf", false);
    }

    if (pgHbaConf is not null)
    {
        CopyConfFile(pgHbaConf, "pg_hba.conf", false);
    }
}

// This function checks if provided datadir is clone of the remote node described by the remote info, or if it's
// emtpy directory that can be used as new datadir.
bool CheckDataDir(string dataDir)
{
    // Run basebackup as needed.
    try
    {
        if (!Directory.Exists(dataDir) || !Directory.EnumerateFileSystemEntries(dataDir).Any())
        {
            // Does not exist, or exists and empty
            return false;
        }
    }
    catch (Exception error) when (error is IOException or UnauthorizedAccessException)
    {
        // Access problem
        throw Die($"Could not access directory \"{dataDir}\": {error.Message}.\n");
    }

    // Exists, not empty
    if (!IsPgDir(dataDir))
    {
        throw Die($"Directory \"{dataDir}\" exists but is not valid postgres data directory.\n");
    }

    return true;
}

// Initialize replication slots.
string InitializeReplicationSlot(NpgsqlConnection conn, string dbname, string providerNodeName,
    string subscriptionName, bool dropSlotIfExists)
{
    // Generate the slot name.
    string slotName;
    try
    {
        slotName = QueryValue(conn,
            $"SELECT worf.worf_gen_slot_name({EscapeLiteral(dbname)}, {EscapeLiteral(providerNodeName)}, {EscapeLiteral(subscriptionName)})")!;
    }
    catch (NpgsqlException error)
    {
        throw Die($"Could generate slot name: {ErrorMessage(error)}");
    }

    // Check if the current slot exists.
    bool exists;
    try
    {
        exists = Query(conn, $"SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = {EscapeLiteral(slotName)}").Count > 0;
    }
    catch (NpgsqlException error)
    {
        throw Die($"Could not fetch existing slot information: {ErrorMessage(error)}");
    }

    // Drop the existing slot when asked for it or error if it already exists.
    if (exists)
    {
        if (!dropSlotIfExists)
        {
            throw Die($"Slot {slotName} already exists, drop it or use --drop-slot-if-exists to drop it automatically.\n");
        }

        PrintMsg(VerbosityLevel.Verbose, $"Droping existing slot {slotName} ...\n");

        try
        {
            _ = Query(conn, $"SELECT pg_catalog.pg_drop_replication_slot({EscapeLiteral(slotName)})");
        }
        catch (NpgsqlException error)
        {
            throw Die($"Could not drop existing slot {slotName}: {ErrorMessage(error)}");
        }
    }

    // And finally, create the slot.
    try
    {
        _ = Query(conn, $"SELECT pg_create_logical_replication_slot({EscapeLiteral(slotName)}, 'worf');");
    }
    catch (NpgsqlException error)
    {
        throw Die($"Could not create replication slot, status PGRES_FATAL_ERROR: {ResultErrorMessage(error)}\n");
    }

    return slotName;
}

// Read replication info about remote connection.
// TODO: unify with worf_remote_node_info in Rpc.
RemoteInfo GetRemoteInfo(NpgsqlConnection conn)
{
    if (!ExtensionExists(conn, "worf"))
    {
        throw Die("The remote node is not configured as a worf provider.\n");
    }

    List<string?[]> res;
    try
    {
        res = Query(conn, "SELECT node_id, node_name, sysid, dbname, replication_sets FROM worf.worf_node_info()");
    }
    catch (NpgsqlException error)
    {
        throw Die($"could not fetch remote node info: {ErrorMessage(error)}\n");
    }

    // No nodes found?
    if (res.Count == 0)
    {
        throw Die("The remote database is not configured as a worf node.\n");
    }

    if (res.Count > 1)
    {
        throw Die("The remote database has multiple nodes configured. That is not supported with current version of worf.\n");
    }

    string?[] row = res[0];
    return new RemoteInfo(uint.Parse(row[0]!, CultureInfo.InvariantCulture), row[1] ?? "", row[2] ?? "", row[3] ?? "", row[4] ?? "");
}

// Check if extension exists.
bool ExtensionExists(NpgsqlConnection conn, string extname)
{
    try
    {
        return Query(conn, $"SELECT 1 FROM pg_catalog.pg_extension WHERE extname = {EscapeLiteral(extname)};").Count == 1;
    }
    catch (NpgsqlException error)
    {
        throw Die($"Could not read extension info: {ErrorMessage(error)}\n");
    }
}

// Create extension.
void InstallExtension(NpgsqlConnection conn, string extname)
{
    try
    {
        _ = Query(conn, $"CREATE EXTENSION IF NOT EXISTS {EscapeIdentifier(extname)};");
    }
    catch (NpgsqlException error)
    {
        throw Die($"Could not install {extname} extension: {ErrorMessage(error)}\n");
    }
}

// Clean all the data that was copied from remote node but we don't want it here (currently shared security labels
// and replication identifiers).
void RemoveUnwantedData(NpgsqlConnection conn)
{
    // Remove replication identifiers.
    try
    {
        _ = Query(conn, "SELECT pg_replication_origin_drop(external_id) FROM pg_replication_origin_status;");
    }
    catch (NpgsqlException error)
    {
        throw Die($"Could not remove existing replication origins: {ErrorMessage(error)}\n");
    }

    try
    {
        _ = Query(conn, "DROP EXTENSION worf CASCADE;");
    }
    catch (NpgsqlException error)
    {
        throw Die($"Could not clean the worf extension, status PGRES_FATAL_ERROR: {ResultErrorMessage(error)}\n");
    }
}

// Initialize new remote identifier to specific position.
void InitializeReplicationOrigin(NpgsqlConnection conn, string originName, string? remoteLsn)
{
    string query = $"SELECT pg_replication_origin_create({EscapeLiteral(originName)})";
    try
    {
        _ = Query(conn, query);
    }
    catch (NpgsqlException error)
    {
        throw Die($"Could not create replication origin \"{query}\": status PGRES_FATAL_ERROR: {ResultErrorMessage(error)}\n");
    }

    if (remoteLsn is not null)
    {
        query = $"SELECT pg_replication_origin_advance({EscapeLiteral(originName)}, '{remoteLsn}')";
        try
        {
            _ = Query(conn, query);
        }
        catch (NpgsqlException error)
        {
            throw Die($"Could not advance replication origin \"{query}\": status PGRES_FATAL_ERROR: {ResultErrorMessage(error)}\n");
        }
    }
}

// Create remote restore point which will be used to get into synchronized state through physical replay.
string? CreateRestorePoint(NpgsqlConnection conn, string restorePointName)
{
    try
    {
        return QueryValue(conn, $"SELECT pg_create_restore_point('{restorePointName}')");
    }
    catch (NpgsqlException error)
    {
        throw Die($"Could not create restore point, status PGRES_FATAL_ERROR: {ResultErrorMessage(error)}\n");
    }
}

void WorfSubscribe(NpgsqlConnection conn, string subscriberName, string subscriberDsn, string providerDsn,
    string replicationSets, int applyDelay, bool forceTextTransfer)
{
    try
    {
        _ = Query(conn,
            $"SELECT worf.create_node(node_name := {EscapeLiteral(subscriberName)}, dsn := {EscapeLiteral(subscriberDsn)});");
    }
    catch (NpgsqlException error)
    {
        throw Die($"Could not create local node, status PGRES_FATAL_ERROR: {ResultErrorMessage(error)}\n");
    }

    string repsets = $"{{{replicationSets}}}";
    try
    {
        _ = Query(conn,
            $"SELECT worf.create_subscription(subscription_name := {EscapeLiteral(subscriberName)}, " +
            $"provider_dsn := {EscapeLiteral(providerDsn)}, " +
            $"replication_sets := {EscapeLiteral(repsets)}, " +
            $"apply_delay := '{applyDelay} seconds'::interval, " +
            "synchronize_structure := false, " +
            "synchronize_data := false, " +
            $"force_text_transfer := '{(forceTextTransfer ? "t" : "f")}');");
    }
    catch (NpgsqlException error)
    {
        throw Die($"Could not create subscription, status PGRES_FATAL_ERROR: {ResultErrorMessage(error)}\n");
    }

    // TODO
    try
    {
        _ = Query(conn, "UPDATE worf.local_sync_status SET sync_status = 'r'");
    }
    catch (NpgsqlException error)
    {
        throw Die($"Could not update subscription, status PGRES_FATAL_ERROR: {ResultErrorMessage(error)}\n");
    }
}

// Validates input of the replication sets and returns normalized data.
string ValidateReplicationSetInput(string replicationSets)
{
    StringBuilder retbuf = new();
    bool first = true;

    foreach (string name in replicationSets.Split([' ', ','], StringSplitOptions.RemoveEmptyEntries))
    {
        if (name.Length > NameDataLen)
        {
            throw Die($"Replication set name \"{name}\" is too long\n");
        }

        foreach (char cp in name)
        {
            if (!(cp is >= 'a' and <= 'z' or >= '0' and <= '9' or '_' or '-'))
            {
                throw Die($"Replication set name \"{name}\" contains invalid character\n");
            }
        }

        if (first)
        {
            first = false;
        }
        else
        {
            retbuf.Append(", ");
        }

        retbuf.Append(name);
    }

    return retbuf.ToString();
}

string? GetConnstrDbname(string connstr)
{
    Dictionary<string, string> connOpts = Fe.ConninfoParse(connstr, out string? errMsg)
        ?? throw Die($"Invalid connection string: {errMsg}\n");

    return connOpts.TryGetValue("dbname", out string? dbname) ? dbname : null;
}

// Build connection string from individual parameter. dbname can be specified in connstr parameter.
string GetConnstr(string connstr, string? dbname)
{
    return Fe.GetConnstr(connstr, dbname, null, out string? errMsg)
        ?? throw Die($"Invalid connection string: {errMsg}\n");
}

// Reads the system identifier from the pg_control file of the existing data dir.
string ReadSysid(string dataDir)
{
    string controlFilePath = Path.Join(dataDir, "global", "pg_control");
    byte[] controlFile = new byte[sizeof(ulong)];

    try
    {
        using FileStream fd = new(controlFilePath, FileMode.Open, FileAccess.Read);
        fd.ReadExactly(controlFile);
    }
    catch (Exception error) when (error is IOException or UnauthorizedAccessException)
    {
        throw Die($"{progname}: could not read file \"{controlFilePath}\": {error.Message}\n");
    }

    // system_identifier is pg_control's first field, in the server's byte order.
    ulong systemIdentifier = BitConverter.IsLittleEndian
        ? BinaryPrimitives.ReadUInt64LittleEndian(controlFile)
        : BinaryPrimitives.ReadUInt64BigEndian(controlFile);
    return systemIdentifier.ToString(CultureInfo.InvariantCulture);
}

// Write contents of postgresql.auto.conf.
void WriteRecoveryConf(string contents)
{
    string filename = Path.Join(dataDir, "postgresql.auto.conf");
    try
    {
        File.AppendAllText(filename, contents);
    }
    catch (Exception error) when (error is IOException or UnauthorizedAccessException)
    {
        throw Die($"{progname}: could not write to file \"{filename}\": {error.Message}\n");
    }

    filename = Path.Join(dataDir, "standby.signal");
    try
    {
        File.WriteAllBytes(filename, []);
    }
    catch (Exception error) when (error is IOException or UnauthorizedAccessException)
    {
        throw Die($"{progname}: could not create file \"{filename}\": {error.Message}\n");
    }
}

// Copy file to data.
void CopyConfFile(string fromfile, string tofile, bool append)
{
    string filename = Path.Join(dataDir, tofile);

    PrintMsg(VerbosityLevel.Debug, $"Copying \"{fromfile}\" to \"{filename}\".\n");
    CopyFile(fromfile, filename, append);
}

// Find the pgport and try a connection.
void WaitPostmasterConnection(string connstr)
{
    long pmpid;

    PrintMsg(VerbosityLevel.Verbose, "Waiting for PostgreSQL to accept connections ...");

    // First wait for Postmaster to come up.
    while (true)
    {
        if ((pmpid = GetPgpid()) != 0 && PostmasterIsAlive(pmpid))
        {
            break;
        }

        Thread.Sleep(TimeSpan.FromSeconds(1));
        PrintMsg(VerbosityLevel.Verbose, ".");
    }

    // Now wait for Postmaster to either accept connections or die.
    while (true)
    {
        PingStatus res = Libpq.Ping(connstr);
        if (res is PingStatus.Ok or PingStatus.NoAttempt)
        {
            break;
        }

        // Check if the process is still alive. This covers cases where the postmaster successfully created the
        // pidfile but then crashed without removing it.
        if (!PostmasterIsAlive(pmpid))
        {
            break;
        }

        // No response; wait
        Thread.Sleep(TimeSpan.FromSeconds(1));
        PrintMsg(VerbosityLevel.Verbose, ".");
    }

    PrintMsg(VerbosityLevel.Verbose, "\n");
}

// Wait for PostgreSQL to leave recovery/standby mode.
void WaitPrimaryConnection(string connstr)
{
    bool ispri = false;
    NpgsqlConnection? conn = null;

    WaitPostmasterConnection(connstr);

    PrintMsg(VerbosityLevel.Verbose, "Waiting for PostgreSQL to become primary...");

    while (!ispri)
    {
        if (conn is null || conn.State != System.Data.ConnectionState.Open)
        {
            conn?.Dispose();
            WaitPostmasterConnection(connstr);
            conn = Connectdb(connstr);
        }

        try
        {
            if (QueryValue(conn, "SELECT pg_is_in_recovery()") == "f")
            {
                ispri = true;
            }
        }
        catch (NpgsqlException)
        {
            // The connection broke; reconnect.
        }

        if (!ispri)
        {
            Thread.Sleep(TimeSpan.FromSeconds(1));
            PrintMsg(VerbosityLevel.Verbose, ".");
        }
    }

    conn!.Dispose();
    PrintMsg(VerbosityLevel.Verbose, "\n");
}

// Wait for postmaster to die.
void WaitPostmasterShutdown()
{
    PrintMsg(VerbosityLevel.Verbose, "Waiting for PostgreSQL to shutdown ...");

    while (GetPgpid() != 0)
    {
        Thread.Sleep(TimeSpan.FromSeconds(1));
        PrintMsg(VerbosityLevel.Normal, ".");
    }

    PrintMsg(VerbosityLevel.Verbose, "\n");
}

bool FileExists(string path) => File.Exists(path) || Directory.Exists(path);

bool IsPgDir(string path)
    => Directory.Exists(path) && File.Exists(Path.Join(dataDir, "PG_VERSION"));

// copy one file.
void CopyFile(string fromfile, string tofile, bool append)
{
    FileStream srcfd;
    try
    {
        srcfd = new FileStream(fromfile, FileMode.Open, FileAccess.Read);
    }
    catch (Exception error) when (error is IOException or UnauthorizedAccessException)
    {
        throw Die($"could not open file \"{fromfile}\"");
    }

    using (srcfd)
    {
        FileStream dstfd;
        try
        {
            dstfd = new FileStream(tofile, append ? FileMode.Append : FileMode.Create, FileAccess.Write);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            throw Die($"could not create file \"{tofile}\"");
        }

        using (dstfd)
        {
            try
            {
                srcfd.CopyTo(dstfd);
            }
            catch (IOException)
            {
                throw Die($"could not write to file \"{tofile}\"");
            }
        }
    }
}

string FindOtherExecOrDie(string argv0, string target)
{
    int ret = Fe.FindOtherExecVersion(argv0, target, out uint binVersion, out string foundPath);
    string fullPath = Path.GetFullPath(argv0);

    if (ret < 0)
    {
        if (ret == -1)
        {
            throw Die($"The program \"{target}\" is needed by {progname} but was not found in the\n" +
                $"same directory as \"{fullPath}\".\nCheck your installation.\n");
        }
        else
        {
            throw Die($"The program \"{target}\" was found by \"{fullPath}\"\nbut was not the same version as {progname}.\n" +
                "Check your installation.\n");
        }
    }

    _ = binVersion;
    return foundPath;
}

bool PostmasterIsAlive(long pid)
{
    // Test to see if the process is still there. Don't believe that our own PID is the postmaster.
    if (pid == Environment.ProcessId)
    {
        return false;
    }

    try
    {
        using Process process = Process.GetProcessById((int)pid);
        return !process.HasExited;
    }
    catch (ArgumentException)
    {
        return false;
    }
}

long GetPgpid()
{
    try
    {
        using StreamReader pidf = new(pidFile);
        return long.TryParse(pidf.ReadLine(), NumberStyles.Integer, CultureInfo.InvariantCulture, out long pid) ? pid : 0;
    }
    catch (Exception error) when (error is IOException or UnauthorizedAccessException or ArgumentException)
    {
        return 0;
    }
}

string[] GetDatabaseList(string databases)
    => databases.Split(',', StringSplitOptions.RemoveEmptyEntries);

string GenerateRestorePointName()
    => $"worf_create_subscriber_{Random.Shared.NextInt64(0, int.MaxValue):x}";

int RunProgram(string execPath, IEnumerable<string> arguments)
{
    ProcessStartInfo start = new(execPath) { UseShellExecute = false };
    foreach (string argument in arguments)
    {
        start.ArgumentList.Add(argument);
    }

    using Process process = Process.Start(start)!;
    process.WaitForExit();
    return process.ExitCode;
}

string FormatCommand(string execPath, IEnumerable<string> arguments)
    => string.Join(' ', [Quote(execPath), .. arguments.Select(Quote)]);

string Quote(string argument)
    => argument.Length > 0 && argument.All(static c => !char.IsWhiteSpace(c) && c is not ('"' or '\''))
        ? argument
        : "\"" + argument + "\"";

// Splits extra arguments as a shell does, honoring single and double quotes.
List<string> SplitArguments(string arguments)
{
    List<string> result = [];
    StringBuilder current = new();
    bool inWord = false;
    char quote = '\0';

    foreach (char c in arguments)
    {
        if (quote != '\0')
        {
            if (c == quote)
            {
                quote = '\0';
            }
            else
            {
                current.Append(c);
            }
        }
        else if (c is '"' or '\'')
        {
            quote = c;
            inWord = true;
        }
        else if (char.IsWhiteSpace(c))
        {
            if (inWord)
            {
                result.Add(current.ToString());
                current.Clear();
                inWord = false;
            }
        }
        else
        {
            current.Append(c);
            inWord = true;
        }
    }

    if (inWord)
    {
        result.Add(current.ToString());
    }

    return result;
}

int Atoi(string value)
{
    int end = 0;
    while (end < value.Length && (char.IsAsciiDigit(value[end]) || (end == 0 && value[end] is '-' or '+')))
    {
        end++;
    }

    return int.TryParse(value.AsSpan(0, end), NumberStyles.Integer, CultureInfo.InvariantCulture, out int result) ? result : 0;
}

// Doubles single quotes and backslashes, as escape_single_quotes_ascii does.
string EscapeSingleQuotesAscii(string value)
    => value.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("'", "''", StringComparison.Ordinal);

// Quotes a string literal, as PQescapeLiteral does.
string EscapeLiteral(string value)
    => value.Contains('\\', StringComparison.Ordinal)
        ? " E'" + value.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("'", "''", StringComparison.Ordinal) + "'"
        : "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

// Quotes an identifier, as PQescapeIdentifier does.
string EscapeIdentifier(string value)
    => "\"" + value.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

List<string?[]> Query(NpgsqlConnection conn, string sql)
{
    // Read every column as text, as PQexec returns it.
    using NpgsqlCommand command = new(sql, conn) { AllResultTypesAreUnknown = true };
    using NpgsqlDataReader reader = command.ExecuteReader();
    List<string?[]> rows = [];
    while (reader.Read())
    {
        string?[] row = new string?[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
        {
            row[i] = reader.IsDBNull(i) ? null : reader.GetString(i);
        }

        rows.Add(row);
    }

    return rows;
}

string? QueryValue(NpgsqlConnection conn, string sql)
{
    List<string?[]> rows = Query(conn, sql);
    return rows.Count > 0 ? rows[0][0] : null;
}

// Formats an error as PQresultErrorMessage does.
string ResultErrorMessage(NpgsqlException error)
{
    if (error is not PostgresException postgres)
    {
        return error.Message + "\n";
    }

    StringBuilder message = new();
    message.Append(CultureInfo.InvariantCulture, $"{postgres.Severity}:  {postgres.MessageText}\n");
    if (postgres.Detail is { } detail)
    {
        message.Append(CultureInfo.InvariantCulture, $"DETAIL:  {detail}\n");
    }

    if (postgres.Hint is { } hint)
    {
        message.Append(CultureInfo.InvariantCulture, $"HINT:  {hint}\n");
    }

    return message.ToString();
}

// Formats an error as PQerrorMessage does.
string ErrorMessage(NpgsqlException error) => ResultErrorMessage(error);
