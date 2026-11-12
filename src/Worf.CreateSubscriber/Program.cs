// worf_create_subscriber turns a physical standby, or a new base backup, of a provider into a worf subscriber.

using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using Worf;
using Worf.CreateSubscriber;

const int MaxApplyDelay = 86400;
const int NameDataLen = 64;
const string LogFileName = "worf_create_subscriber_postgres.log";
const string ServerLogFileName = "worf_subscriber.log";

// DB_SHUTDOWNED from catalog/pg_control.h: the cluster state of a server that stopped out of recovery.
const int ClusterShutDown = 1;
string argv0 = Environment.ProcessPath ?? Environment.GetCommandLineArgs()[0];
string progname = Path.GetFileNameWithoutExtension(argv0);
string? dataDir = null;
string pidFile = "";
VerbosityLevel verbosity = VerbosityLevel.Normal;

// The connection whose command runs, which a stop cancels, and the program that runs, which a stop ends.
LibpqConnection? running = null;

// Whether a stop came, which the main thread acts on, and whether a database's subscription has committed, after which a
// stop waits for the run to set the other databases up, as their subscriptions can no longer all roll back.
using ManualResetEventSlim stopped = new(false);
bool committed = false;
Lock committing = new();

// How many of the databases have their subscription committed, in the order the run sets them up, and the slots the
// run made on the provider for each, once it has begun making them.
int subscribedDatabases = 0;
(string Database, string? Slot, string? Lsn)[]? madeSlots = null;

// Whether this run started the subscriber's server, which it then stops if it fails, and whether it is failing, which a
// failure on another thread waits out.
bool serverStarted = false;
int dying = 0;

// postgresql.auto.conf as it was before this run added its recovery settings, which a run that fails before the server
// leaves recovery puts back, with no standby.signal unless the data directory had one, so that it starts as it did.
byte[]? autoConfBefore = null;
bool hadStandbySignal = false;
bool promoted = false;

// postgresql.conf and pg_hba.conf as they were before this run copied --postgresql-conf and --hba-conf over them, which a
// run that fails before the server leaves recovery puts back too.
byte[]? postgresqlConfBefore = null;
byte[]? hbaConfBefore = null;

// The directories pg_basebackup writes outside the data directory, the WAL's and the tablespaces' that its arguments map,
// which a stop during the backup removes, if the backup made them, or empties, if they were there and empty, as
// pg_basebackup writes into no other.
HashSet<string> madeOutside = [];
HashSet<string> emptyOutside = [];

// Whether pg_basebackup is making the data directory, which a stop meanwhile removes, and whether it was there before.
bool backingUp = false;
bool dataDirExisted = false;

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

// A stop, as Ctrl-C or SIGTERM, cancels the command that runs, so that it fails, and the main thread, at its next step,
// or in the wait, connection, command or program it's in, cleans up as on any failure, rolling back its transaction; a
// stop while it does changes nothing, and neither does one once a subscription has committed, but for the run to end
// once it has set the other databases up.
void Stop(PosixSignalContext context)
{
    context.Cancel = true;
    LibpqConnection? cancelling;
    lock (committing)
    {
        stopped.Set();
        if (committed)
        {
            return;
        }

        // The command running as the stop came, read before a commit can mark it committed, so that the cancel lands on
        // no command the run starts after.
        cancelling = running;
    }

    _ = cancelling?.Cancel();
}

// Fails, from the main thread, once a stop has come.
void CheckStop()
{
    if (stopped.IsSet && !committed)
    {
        throw Die("\nCanceling...\n");
    }
}

// Waits, unless a stop comes first.
void Pause(TimeSpan time)
{
    _ = stopped.Wait(time);
    CheckStop();
}

List<PosixSignalRegistration> stops = [];
foreach (PosixSignal signal in new[] { PosixSignal.SIGINT, PosixSignal.SIGTERM, PosixSignal.SIGQUIT })
{
    try
    {
        stops.Add(PosixSignalRegistration.Create(signal, Stop));
    }
    catch (PlatformNotSupportedException)
    {
        // Windows has no SIGQUIT.
    }
}

// A registration that nothing references any more is collected, and its finalizer unregisters the handler, so that a
// signal then ends the run before it cleans up; the process's exit keeps them.
AppDomain.CurrentDomain.ProcessExit += (_, _) => GC.KeepAlive(stops);

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
madeSlots = new (string Database, string? Slot, string? Lsn)[databaseList.Length];

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
    using LibpqConnection providerConn = Connectdb(provConnstr);
    RemoteInfo remoteInfo = GetRemoteInfo(providerConn);

    // only need to do this piece once
    if (dbnum == 0)
    {
        useExistingDataDir = CheckDataDir(dataDir);

        if (useExistingDataDir && remoteInfo.Sysid != ReadSysid(dataDir))
        {
            throw Die("Subscriber data directory is not basebackup of remote node.\n");
        }

        // A server running in the data directory, as one an earlier run left, is left as it is, and so is a data directory
        // whose timeline is past the provider's, as one an earlier run promoted, whose recovery can't reach the restore
        // point.
        pidFile = Path.Join(dataDir, "postmaster.pid");
        if (useExistingDataDir && GetPgpid() is long serverPid and not 0 && PostmasterIsAlive(serverPid))
        {
            throw Die($"A server is running in data directory \"{dataDir}\"; stop it before running this again.\n");
        }

        (long? timeline, string? state, ulong? checkpoint, ulong? redo, long? segmentSize, long? blockSize) = useExistingDataDir
            ? ReadControlData(dataDir) : (null, null, null, null, null, null);
        if (timeline is long dataTimeline &&
            long.TryParse(QueryValue(providerConn, "SELECT timeline_id FROM pg_catalog.pg_control_checkpoint()"), NumberStyles.Integer,
                CultureInfo.InvariantCulture, out long providerTimeline) && dataTimeline > providerTimeline)
        {
            throw Die($"Data directory \"{dataDir}\" is on timeline {dataTimeline}, past the provider's timeline {providerTimeline}, as when an earlier run promoted it, so it can't recover to the provider's restore point; make a new base backup.\n");
        }

        // A data directory that ran as a server of its own, out of recovery, as one started without its standby.signal
        // does, has written WAL the provider doesn't have, though on the provider's timeline. A base backup not yet
        // started has its backup_label, and a standby that stopped says it was in recovery.
        if (state is "shut down" or "shutting down" or "in production" or "in crash recovery" && !File.Exists(Path.Join(dataDir, "backup_label")))
        {
            throw Die($"Data directory \"{dataDir}\" has run as a server of its own, as its cluster state \"{state}\" shows, so its WAL may have left the provider's; make a new base backup.\n");
        }

        // So has one whose latest checkpoint isn't the provider's, though it stopped in recovery since, as one started
        // again with its standby.signal does: a standby's restart points are at the checkpoint records it replayed, so
        // one that only ever replayed the provider's WAL has, at its latest checkpoint, a checkpoint record of the
        // provider's with the redo point its pg_control holds, and one that wrote WAL of its own has its own, which no
        // WAL streamed over its own since takes away. A data directory that has started, whose latest checkpoint's WAL the
        // provider has removed, can't be told apart from one that left the provider's, and may need WAL the provider no
        // longer has to recover. A base backup that hasn't started yet holds the provider's checkpoint.
        if (!File.Exists(Path.Join(dataDir, "backup_label")) && timeline is long checkpointTimeline && checkpoint is ulong latest && redo is ulong redoPoint &&
            segmentSize is long segmentBytes and > 0 && blockSize is long blockBytes and > 0 &&
            WalJoins(providerConn, checkpointTimeline, latest, redoPoint, segmentBytes, blockBytes) is (false, string why))
        {
            throw Die($"Data directory \"{dataDir}\"'s latest checkpoint, at {latest >> 32:X}/{latest & 0xFFFFFFFF:X}, {why}; make a new base backup.\n");
        }
    }

    // Create replication slots on remote node.
    PrintMsg(VerbosityLevel.Normal, $"Creating replication slot in database {db} ...\n");
    (slotNames[dbnum], string madeAt) = InitializeReplicationSlot(providerConn, remoteInfo.Dbname, remoteInfo.NodeName,
        subscriberName, dropSlotIfExists);
    madeSlots[dbnum] = (db, slotNames[dbnum], madeAt);
}

// Create basebackup or use existing one
string prov = GetConnstr(baseProvConnstr, databaseList[0]);
string sub = GetConnstr(baseSubConnstr, databaseList[0]);

InitializeDataDir(dataDir, useExistingDataDir ? null : prov, postgresqlConf, pgHbaConf, extraBasebackupArgs);
pidFile = Path.Join(dataDir, "postmaster.pid");

string restorePointName = GenerateRestorePointName();

PrintMsg(VerbosityLevel.Normal, $"Creating restore point \"{restorePointName}\" on remote node ...\n");
using (LibpqConnection restoreConn = Connectdb(prov))
{
    remoteLsn = CreateRestorePoint(restoreConn, restorePointName);
}

// Get subscriber db to consistent state (for lsn after slot creation).
PrintMsg(VerbosityLevel.Normal, "Bringing subscriber node to the restore point ...\n");
SaveRecoverySettings();
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
serverStarted = true;
pgCtlRet = RunPgCtl(["start", "-l", LogFileName, "-o", "-c shared_preload_libraries="]);
if (pgCtlRet != 0)
{
    throw Die($"Postgres startup for restore point catchup failed with {pgCtlRet}. See {LogFileName}.");
}

WaitPrimaryConnection(sub, LogFileName);

// Clean any per-node data that were copied by pg_basebackup.
PrintMsg(VerbosityLevel.Verbose, "Removing old worf configuration ...\n");

foreach (string subConnstr in databaseList.Select(db => GetConnstr(baseSubConnstr, db)))
{
    if (string.IsNullOrEmpty(subConnstr))
    {
        throw Die("Subscriber connection string is not valid.\n");
    }

    using LibpqConnection cleaningConn = Connectdb(subConnstr);
    RemoveUnwantedData(cleaningConn);
}

// Stop Postgres so we can reset system id and start it with worf loaded.
pgCtlRet = RunPgCtl(["stop"]);
if (pgCtlRet != 0)
{
    throw Die($"Postgres stop after restore point catchup failed with {pgCtlRet}. See {LogFileName}.");
}

WaitPostmasterShutdown();

// Start the node again, now with worf active so that we can start the logical replication. The server, which runs on
// after the run, logs to a file in its data directory rather than to the run's output, which it would otherwise hold
// open.
PrintMsg(VerbosityLevel.Normal, "Initializing worf on the subscriber node:\n");

serverStarted = true;
pgCtlRet = RunPgCtl(["start", "-l", Path.Join(dataDir, ServerLogFileName)]);
if (pgCtlRet != 0)
{
    throw Die($"Postgres restart with worf enabled failed with {pgCtlRet}. See {Path.Join(dataDir, ServerLogFileName)}.");
}

WaitPostmasterConnection(baseSubConnstr, Path.Join(dataDir, ServerLogFileName));

for (int dbnum = 0; dbnum < databaseList.Length; dbnum++)
{
    string db = databaseList[dbnum];

    string subConnstr = GetConnstr(baseSubConnstr, db);
    string provConnstr = GetConnstr(baseProvConnstr, db);

    using LibpqConnection subscriberConn = Connectdb(subConnstr);

    // Create the extension.
    PrintMsg(VerbosityLevel.Verbose, $"Creating worf extension for database {db}...\n");
    InstallExtension(subscriberConn, "worf");

    // Create the identifier which is setup with the position to which we already caught up using physical
    // replication.
    // The origin, the node and the subscription are made in one transaction, so that the subscription takes the origin
    // as its own, and its apply worker starts only once its sync status says it needs no copy. The sync status holds the
    // position to start from, which the apply worker starts from while the origin has none, so that the origin gets its
    // position only as the worker applies, and a transaction that rolls back, as when the tool is stopped or killed,
    // leaves nothing, as PostgreSQL keeps an origin's progress for its id however its transaction ends, and an origin
    // created later with the id would start from it.
    _ = Query(subscriberConn, "BEGIN");
    PrintMsg(VerbosityLevel.Verbose, $"Creating replication origin for database {db}...\n");
    InitializeReplicationOrigin(subscriberConn, slotNames[dbnum]);

    // And finally add the node to the cluster.
    PrintMsg(VerbosityLevel.Normal, $"Creating subscriber {subscriberName} for database {db}...\n");
    PrintMsg(VerbosityLevel.Verbose, $"Replication sets: {replicationSets}\n");

    WorfSubscribe(subscriberConn, subscriberName, subConnstr, provConnstr, replicationSets, applyDelay,
        forceTextTransfer, remoteLsn);

    // The commit runs to its end, which a stop meanwhile cancels or leaves done, and the run follows what it did. Once a
    // database's subscription has committed, the others' can't roll back with it, so the run goes on to set them up.
    CheckStop();
    try
    {
        running = subscriberConn;
        _ = subscriberConn.Query("COMMIT");
    }
    catch (LibpqException error)
    {
        throw Die($"Could not commit the subscription, status PGRES_FATAL_ERROR: {error.Message}\n");
    }
    finally
    {
        running = null;
    }

    lock (committing)
    {
        committed = true;
        subscribedDatabases = dbnum + 1;
    }
}

// If user does not want the node to be running at the end, stop it.
if (stop)
{
    PrintMsg(VerbosityLevel.Normal, "Stopping the subscriber node ...\n");
    pgCtlRet = RunPgCtl(["stop"]);
    serverStarted = false;
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

// Print error and exit, stopping the server this run started. Once, from whichever thread fails or stops first: another
// waits for it, as the process exits.
// Returns nothing; the process exits. Callers throw the result so the compiler knows execution stops.
InvalidOperationException Die(string message)
{
    if (Interlocked.Exchange(ref dying, 1) != 0)
    {
        Thread.Sleep(Timeout.Infinite);
    }

    Console.Error.Write(message);

    // The connections are left to close as the process exits, as a call another thread makes on one may still run.
    // A base backup that a stop ended is removed, so that the data directory is as it was.
    if (backingUp)
    {
        try
        {
            RemoveOutside();
            if (dataDirExisted)
            {
                foreach (string entry in Directory.EnumerateFileSystemEntries(dataDir!))
                {
                    if (Directory.Exists(entry))
                    {
                        Directory.Delete(entry, true);
                    }
                    else
                    {
                        File.Delete(entry);
                    }
                }
            }
            else if (Directory.Exists(dataDir))
            {
                Directory.Delete(dataDir!, true);
            }
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            Console.Error.WriteLine($"WARNING: could not remove the base backup made in \"{dataDir}\": {error.Message}");
        }
    }

    bool stoppedServer = true;
    if (serverStarted && GetPgpid() != 0 && RunProgramToEnd(FindOtherExecOrDie(argv0, "pg_ctl"), ["stop", "-s", "-D", dataDir!]) != 0)
    {
        stoppedServer = false;
        Console.Error.WriteLine("WARNING: postgres seems to be running, but could not be stopped");
    }

    // A server that never left recovery leaves the data directory as it was, without the settings this run added or
    // the configuration files it copied. The server may have left recovery before the run saw it, as when it reached
    // the restore point just as the stop came, which its cluster state, once stopped, shows.
    bool leftRecovery = promoted || (serverStarted && stoppedServer && ReadClusterState(dataDir!) == ClusterShutDown);
    if (!leftRecovery && stoppedServer && !backingUp)
    {
        try
        {
            if (autoConfBefore is not null)
            {
                File.WriteAllBytes(Path.Join(dataDir, "postgresql.auto.conf"), autoConfBefore);
                if (!hadStandbySignal)
                {
                    File.Delete(Path.Join(dataDir, "standby.signal"));
                }
            }

            if (postgresqlConfBefore is not null)
            {
                File.WriteAllBytes(Path.Join(dataDir, "postgresql.conf"), postgresqlConfBefore);
            }

            if (hbaConfBefore is not null)
            {
                File.WriteAllBytes(Path.Join(dataDir, "pg_hba.conf"), hbaConfBefore);
            }
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            Console.Error.WriteLine($"WARNING: could not take this run's recovery settings out of \"{dataDir}\": {error.Message}");
        }
    }

    DropUnusedSlots();
    Environment.Exit(1);
    return new InvalidOperationException(message);
}

// Drops the slots this run made on the provider for the databases it didn't subscribe, which nothing else would use, so
// that a failed or stopped run leaves the provider as it was. A slot of the name whose confirmed position isn't the one
// this run's slot was made at, as one a later run with the same subscriber name made in its place, or this run's once a
// client streamed from it, a slot a walsender holds, or a provider that doesn't answer within a few seconds, is left,
// with a warning.
void DropUnusedSlots()
{
    int subscribed;
    lock (committing)
    {
        subscribed = subscribedDatabases;
    }

    for (int dbnum = subscribed; dbnum < (madeSlots?.Length ?? 0); dbnum++)
    {
        if (madeSlots![dbnum] is not (string database, string slot, string lsn) || baseProvConnstr is null ||
            Fe.GetConnstr(baseProvConnstr, database, null, out _) is not string connstr)
        {
            continue;
        }

        try
        {
            using LibpqConnection conn = Libpq.Connect(connstr + " connect_timeout=5");
            string made = $"slot_name = {EscapeLiteral(slot)} AND confirmed_flush_lsn = {EscapeLiteral(lsn)}::pg_catalog.pg_lsn";
            List<string?[]> found = conn.Query($"""
                SELECT (SELECT active_pid FROM pg_catalog.pg_replication_slots WHERE slot_name = {EscapeLiteral(slot)} AND active),
                    EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = {EscapeLiteral(slot)} AND NOT ({made})),
                    (SELECT count(pg_catalog.pg_drop_replication_slot(slot_name)) FROM pg_catalog.pg_replication_slots WHERE {made} AND NOT active)
                """);
            string streaming = found[0][0] is string pid ? $", and process {pid} streams from it" : "";
            if (found[0][1] == "t")
            {
                Console.Error.WriteLine($"WARNING: replication slot \"{slot}\" on the provider isn't as this run made it, as its confirmed position shows{streaming}: another run with the same subscriber name may have made it in its place, or a client streamed from it, so it's left there");
            }
            else if (found[0][0] is string user)
            {
                Console.Error.WriteLine($"WARNING: replication slot \"{slot}\" on the provider is in use by process {user}, so it's left there; drop it with pg_drop_replication_slot once nothing uses it");
            }
        }
        catch (Exception error) when (error is LibpqException or DllNotFoundException)
        {
            Console.Error.WriteLine($"WARNING: could not drop replication slot \"{slot}\" on the provider: {error.Message.Trim()}");
        }
    }
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

LibpqConnection Connectdb(string connstr)
{
    CheckStop();
    try
    {
        return Interruptible(() => Libpq.Connect(connstr));
    }
    catch (LibpqException error)
    {
        throw Die($"Connection to database failed: {error.Message}, connection string was: {connstr}\n");
    }
    catch (DllNotFoundException error)
    {
        throw Die($"could not load libpq: {error.Message}\n");
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
    return RunProgram(execPath, arguments, false, relayed: true);
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
    dataDirExisted = Directory.Exists(dataDir);
    foreach (string outside in OutsideDirectories(arguments))
    {
        // One that can't be listed is left to pg_basebackup, which refuses it.
        if (!Directory.Exists(outside))
        {
            _ = madeOutside.Add(outside);
        }
        else if (Listable(outside) is false)
        {
            _ = emptyOutside.Add(outside);
        }
    }

    backingUp = true;
    int ret = RunProgram(execPath, arguments, true);
    backingUp = false;

    if (ret != 0)
    {
        throw Die($"pg_basebackup failed with exit status {ret}, cannot continue.\n");
    }
}

// The directories pg_basebackup's arguments have it write outside the data directory: the WAL's, as --waldir names it,
// and the tablespaces' that -T maps, as full paths.
IEnumerable<string> OutsideDirectories(List<string> arguments)
{
    for (int i = 0; i < arguments.Count; i++)
    {
        string argument = arguments[i];
        string? value = null;
        bool tablespace = false;
        foreach ((string option, bool mapping) in new[] { ("--waldir", false), ("--xlogdir", false), ("--tablespace-mapping", true) })
        {
            if (argument == option && i + 1 < arguments.Count)
            {
                value = arguments[++i];
                tablespace = mapping;
            }
            else if (argument.StartsWith(option + "=", StringComparison.Ordinal))
            {
                value = argument[(option.Length + 1)..];
                tablespace = mapping;
            }
        }

        if (value is null && argument.StartsWith("-T", StringComparison.Ordinal))
        {
            value = argument.Length > 2 ? argument[2..] : i + 1 < arguments.Count ? arguments[++i] : null;
            tablespace = true;
        }

        // A mapping's new directory follows its first unescaped "=".
        if (value is not null && tablespace)
        {
            int equals = -1;
            for (int j = 0; j < value.Length && equals < 0; j++)
            {
                if (value[j] == '\\')
                {
                    j++;
                }
                else if (value[j] == '=')
                {
                    equals = j;
                }
            }

            value = equals < 0 ? null : value[(equals + 1)..];
        }

        if (value is { Length: > 0 })
        {
            yield return Path.GetFullPath(value);
        }
    }
}

// Whether a directory has entries, or null when it can't be listed.
static bool? Listable(string directory)
{
    try
    {
        return Directory.EnumerateFileSystemEntries(directory).Any();
    }
    catch (Exception error) when (error is IOException or UnauthorizedAccessException)
    {
        return null;
    }
}

// Removes what a stopped base backup wrote outside the data directory, through the links it left there: the WAL
// directory pg_wal links to and the tablespaces pg_tblspc links to, each emptied, or removed if the backup made it.
void RemoveOutside()
{
    List<string> links = [Path.Join(dataDir, "pg_wal")];
    string tablespaces = Path.Join(dataDir, "pg_tblspc");
    if (Directory.Exists(tablespaces))
    {
        links.AddRange(Directory.EnumerateFileSystemEntries(tablespaces));
    }

    foreach (string link in links)
    {
        DirectoryInfo info = new(link);
        if (!info.Exists || info.LinkTarget is not string target)
        {
            continue;
        }

        string full = Path.GetFullPath(target, Path.GetDirectoryName(link)!);
        if (madeOutside.Contains(full))
        {
            Directory.Delete(full, true);
            continue;
        }

        Empty(full);
    }

    // A directory the backup made that it left no link to yet, as one whose tablespace it streamed before the archive
    // that holds the links, goes too, and one that was there, empty, is emptied.
    foreach (string made in madeOutside.Where(Directory.Exists))
    {
        Directory.Delete(made, true);
    }

    foreach (string empty in emptyOutside.Where(Directory.Exists))
    {
        Empty(empty);
    }

    static void Empty(string directory)
    {
        foreach (string entry in Directory.GetFileSystemEntries(directory))
        {
            if (Directory.Exists(entry) && new DirectoryInfo(entry).LinkTarget is null)
            {
                Directory.Delete(entry, true);
            }
            else
            {
                File.Delete(entry);
            }
        }
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
        postgresqlConfBefore ??= ReadConfFile("postgresql.conf");
        CopyConfFile(postgresqlConf, "postgresql.conf", false);
    }

    if (pgHbaConf is not null)
    {
        hbaConfBefore ??= ReadConfFile("pg_hba.conf");
        CopyConfFile(pgHbaConf, "pg_hba.conf", false);
    }
}

// Reads a configuration file of the data directory, byte for byte, as it is before this run changes it.
byte[] ReadConfFile(string name)
{
    string filename = Path.Join(dataDir, name);
    try
    {
        return File.Exists(filename) ? File.ReadAllBytes(filename) : [];
    }
    catch (Exception error) when (error is IOException or UnauthorizedAccessException)
    {
        throw Die($"{progname}: could not read file \"{filename}\": {error.Message}\n");
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

// Initialize replication slots: the slot's name, and the LSN its creation returned, which its confirmed_flush_lsn holds
// until a client confirms what it streamed from it, and which a slot another run makes later can't hold.
(string Slot, string Lsn) InitializeReplicationSlot(LibpqConnection conn, string dbname, string providerNodeName,
    string subscriptionName, bool dropSlotIfExists)
{
    // Generate the slot name.
    string slotName;
    try
    {
        slotName = QueryValue(conn,
            $"SELECT worf.worf_gen_slot_name({EscapeLiteral(dbname)}, {EscapeLiteral(providerNodeName)}, {EscapeLiteral(subscriptionName)})")!;
    }
    catch (LibpqException error)
    {
        throw Die($"Could generate slot name: {error.Message}");
    }

    // Check if the current slot exists.
    bool exists;
    try
    {
        exists = Query(conn, $"SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = {EscapeLiteral(slotName)}").Count > 0;
    }
    catch (LibpqException error)
    {
        throw Die($"Could not fetch existing slot information: {error.Message}");
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
        catch (LibpqException error)
        {
            throw Die($"Could not drop existing slot {slotName}: {error.Message}");
        }
    }

    // And finally, create the slot.
    string lsn;
    try
    {
        lsn = QueryValue(conn, $"SELECT lsn FROM pg_create_logical_replication_slot({EscapeLiteral(slotName)}, 'worf');") ?? "";
    }
    catch (LibpqException error)
    {
        throw Die($"Could not create replication slot, status PGRES_FATAL_ERROR: {error.Message}\n");
    }

    return (slotName, lsn);
}

// Read replication info about remote connection.
// TODO: unify with worf_remote_node_info in Rpc.
RemoteInfo GetRemoteInfo(LibpqConnection conn)
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
    catch (LibpqException error)
    {
        throw Die($"could not fetch remote node info: {error.Message}\n");
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
bool ExtensionExists(LibpqConnection conn, string extname)
{
    try
    {
        return Query(conn, $"SELECT 1 FROM pg_catalog.pg_extension WHERE extname = {EscapeLiteral(extname)};").Count == 1;
    }
    catch (LibpqException error)
    {
        throw Die($"Could not read extension info: {error.Message}\n");
    }
}

// Create extension.
void InstallExtension(LibpqConnection conn, string extname)
{
    try
    {
        _ = Query(conn, $"CREATE EXTENSION IF NOT EXISTS {EscapeIdentifier(extname)};");
    }
    catch (LibpqException error)
    {
        throw Die($"Could not install {extname} extension: {error.Message}\n");
    }
}

// Clean all the data that was copied from remote node but we don't want it here (currently shared security labels
// and replication identifiers).
void RemoveUnwantedData(LibpqConnection conn)
{
    // Remove replication identifiers.
    try
    {
        _ = Query(conn, "SELECT pg_replication_origin_drop(external_id) FROM pg_replication_origin_status;");
    }
    catch (LibpqException error)
    {
        throw Die($"Could not remove existing replication origins: {error.Message}\n");
    }

    // The copy's managed sequences would go on drawing from the source node's blocks once worf is gone, so each stops
    // first, at the value worf's quarantine reserves, which no block holds; the new node joins them again under its own
    // identity. worf isn't loaded here, so plain SQL does it.
    try
    {
        _ = Query(conn, """
            DO $$
            DECLARE
                s record;
                lo bigint;
                hi bigint;
                edge bigint;
            BEGIN
                IF to_regclass('worf.sequence_range') IS NULL THEN
                    RETURN;
                END IF;

                FOR s IN
                    SELECT r.seqoid::regclass AS seq, q.seqincrement > 0 AS ascending, q.seqtypid::regtype::text AS type
                    FROM worf.sequence_range r JOIN pg_catalog.pg_sequence q ON q.seqrelid = r.seqoid
                LOOP
                    lo := CASE s.type WHEN 'smallint' THEN -32768 WHEN 'integer' THEN -2147483648 ELSE -9223372036854775808 END;
                    hi := CASE s.type WHEN 'smallint' THEN 32767 WHEN 'integer' THEN 2147483647 ELSE 9223372036854775807 END;
                    IF s.ascending THEN
                        hi := lo + 1;
                        edge := hi;
                    ELSE
                        lo := hi - 1;
                        edge := lo;
                    END IF;

                    EXECUTE format('ALTER SEQUENCE %s NO CYCLE CACHE 1 MINVALUE %s MAXVALUE %s START WITH %s RESTART WITH %s', s.seq, lo, hi, edge, edge);
                    PERFORM pg_catalog.setval(s.seq, edge, true);
                END LOOP;
            END$$;
            """);
        _ = Query(conn, "DROP EXTENSION worf CASCADE;");
    }
    catch (LibpqException error)
    {
        throw Die($"Could not clean the worf extension, status PGRES_FATAL_ERROR: {error.Message}\n");
    }
}

// Initialize new remote identifier.
void InitializeReplicationOrigin(LibpqConnection conn, string originName)
{
    string query = $"SELECT pg_replication_origin_create({EscapeLiteral(originName)})";
    try
    {
        _ = Query(conn, query);
    }
    catch (LibpqException error)
    {
        throw Die($"Could not create replication origin \"{query}\": status PGRES_FATAL_ERROR: {error.Message}\n");
    }
}

// Create remote restore point which will be used to get into synchronized state through physical replay.
string? CreateRestorePoint(LibpqConnection conn, string restorePointName)
{
    try
    {
        return QueryValue(conn, $"SELECT pg_create_restore_point('{restorePointName}')");
    }
    catch (LibpqException error)
    {
        throw Die($"Could not create restore point, status PGRES_FATAL_ERROR: {error.Message}\n");
    }
}

void WorfSubscribe(LibpqConnection conn, string subscriberName, string subscriberDsn, string providerDsn,
    string replicationSets, int applyDelay, bool forceTextTransfer, string? startLsn)
{
    try
    {
        _ = Query(conn,
            $"SELECT worf.create_node(node_name := {EscapeLiteral(subscriberName)}, dsn := {EscapeLiteral(subscriberDsn)});");
    }
    catch (LibpqException error)
    {
        throw Die($"Could not create local node, status PGRES_FATAL_ERROR: {error.Message}\n");
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
    catch (LibpqException error)
    {
        throw Die($"Could not create subscription, status PGRES_FATAL_ERROR: {error.Message}\n");
    }

    // TODO
    try
    {
        _ = Query(conn, startLsn is null
            ? "UPDATE worf.local_sync_status SET sync_status = 'r'"
            : $"UPDATE worf.local_sync_status SET sync_status = 'r', sync_statuslsn = '{startLsn}'");
    }
    catch (LibpqException error)
    {
        throw Die($"Could not update subscription, status PGRES_FATAL_ERROR: {error.Message}\n");
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

// Reads a data directory's cluster state from its pg_control, without a program to run, as the cleanup needs it: the
// state follows the system identifier and the control file's and catalog's version numbers, in the server's byte order,
// as it has since PostgreSQL 9; or null.
int? ReadClusterState(string dataDir)
{
    byte[] controlFile = new byte[20];
    try
    {
        using FileStream fd = new(Path.Join(dataDir, "global", "pg_control"), FileMode.Open, FileAccess.Read);
        fd.ReadExactly(controlFile);
    }
    catch (Exception error) when (error is IOException or UnauthorizedAccessException)
    {
        return null;
    }

    return BitConverter.IsLittleEndian ? BinaryPrimitives.ReadInt32LittleEndian(controlFile.AsSpan(16)) : BinaryPrimitives.ReadInt32BigEndian(controlFile.AsSpan(16));
}

// Reads the timeline, position and redo point of a data directory's latest checkpoint, its cluster state, and its WAL
// segment and block sizes, as pg_controldata shows them, or nulls.
(long? Timeline, string? State, ulong? Checkpoint, ulong? Redo, long? SegmentSize, long? BlockSize) ReadControlData(string dataDir)
{
    ProcessStartInfo start = new(FindOtherExecOrDie(argv0, "pg_controldata")) { UseShellExecute = false, RedirectStandardOutput = true };
    start.ArgumentList.Add(dataDir);
    start.Environment["LC_ALL"] = "C";
    using Process process = Process.Start(start)!;
    string output = process.StandardOutput.ReadToEnd();
    process.WaitForExit();
    long? timeline = null;
    string? state = null;
    ulong? checkpoint = null;
    ulong? redo = null;
    long? segmentSize = null;
    long? blockSize = null;
    foreach (string line in output.Split('\n'))
    {
        string? value = line.IndexOf(':', StringComparison.Ordinal) is int colon and > 0 ? line[(colon + 1)..].Trim() : null;
        if (line.StartsWith("Latest checkpoint's TimeLineID:", StringComparison.Ordinal) &&
            long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long found))
        {
            timeline = found;
        }
        else if (line.StartsWith("Database cluster state:", StringComparison.Ordinal))
        {
            state = value;
        }
        else if (line.StartsWith("Latest checkpoint location:", StringComparison.Ordinal) && Lsn(value) is ulong location)
        {
            checkpoint = location;
        }
        else if (line.StartsWith("Latest checkpoint's REDO location:", StringComparison.Ordinal) && Lsn(value) is ulong redoLocation)
        {
            redo = redoLocation;
        }
        else if (line.StartsWith("Bytes per WAL segment:", StringComparison.Ordinal) &&
            long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long segment))
        {
            segmentSize = segment;
        }
        else if (line.StartsWith("WAL block size:", StringComparison.Ordinal) &&
            long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long block))
        {
            blockSize = block;
        }
    }

    return (timeline, state, checkpoint, redo, segmentSize, blockSize);

    static ulong? Lsn(string? text)
        => text?.Split('/') is [string high, string low] && uint.TryParse(high, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out uint hi) &&
            uint.TryParse(low, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out uint lo)
            ? ((ulong)hi << 32) | lo : null;
}

// Whether a data directory's WAL is the provider's: true; false with why not; or null when the provider doesn't let the
// run read its WAL. The directory's WAL from its latest checkpoint's redo point on is read as recovery reads it, each
// segment from the latest timeline the directory holds it on, and its records from the checkpoint on must follow each
// other, each linking back to the one before and whole, as its checksum shows. The WAL the directory holds and the
// provider still has, from the redo point to where either ends, must be the provider's byte for byte, and there must be
// some: a directory whose WAL ends where the provider's begins, or before, as a standby whose position the provider
// didn't keep WAL from does, can't be told from one that left the provider's, which a physical replication slot or
// wal_keep_size on the provider prevents. The directory's timelines must be the provider's: a history file of the
// directory's must be the provider's own, and WAL on a timeline the provider has left must end where the provider left
// it. A directory whose WAL left the provider's, as one that ran as a server of its own, or a copy of the provider's
// from before the provider went back to a backup, has records the provider's WAL doesn't, where they overlap, or WAL on a
// timeline of its own.
(bool Shared, string? Why)? WalJoins(LibpqConnection conn, long checkpointTimeline, ulong checkpoint, ulong redo, long segmentSize, long blockSize)
{
    const int RecordHeader = 24;
    const int LongPageHeader = 40;
    const int ShortPageHeader = 24;
    const int Chunk = 16 << 20;
    ulong segmentBytes = (ulong)segmentSize;
    ulong pageBytes = (ulong)blockSize;

    // Where the provider's WAL ends, which any role may read, so that a directory past it is refused whatever else the
    // run's role may read; and the provider's timeline and the files it keeps, which only pg_monitor's members may list.
    ulong providerEnd;
    try
    {
        providerEnd = ParseLsn(QueryValue(conn, """
            SELECT coalesce(CASE WHEN pg_catalog.pg_is_in_recovery()
                    THEN coalesce(pg_catalog.pg_last_wal_receive_lsn(), pg_catalog.pg_last_wal_replay_lsn())
                    ELSE pg_catalog.pg_current_wal_flush_lsn() END, '0/0')::pg_catalog.text
            """)) ?? 0;
    }
    catch (LibpqException)
    {
        return null;
    }

    if (checkpoint > providerEnd)
    {
        return (false, "is past the provider's WAL, so its WAL has left the provider's");
    }

    long providerTimeline;
    Dictionary<string, string> providerSegments = [];
    HashSet<long> providerHistories = [];
    try
    {
        providerTimeline = long.Parse(QueryValue(conn, "SELECT timeline_id FROM pg_catalog.pg_control_checkpoint()") ?? "1", CultureInfo.InvariantCulture);
        foreach (string?[] row in conn.Query("SELECT name FROM pg_catalog.pg_ls_waldir()"))
        {
            if (row[0] is not string name)
            {
                continue;
            }

            if (WalSegments.TryParse(name, segmentSize, out long tli, out _))
            {
                providerTimeline = Math.Max(providerTimeline, tli);
                if (name.Length == 24 || !providerSegments.ContainsKey(name[..24]))
                {
                    providerSegments[name[..24]] = name;
                }
            }
            else if (WalSegments.TryParseHistory(name, out long history))
            {
                providerTimeline = Math.Max(providerTimeline, history);
                _ = providerHistories.Add(history);
            }
        }
    }
    catch (LibpqException)
    {
        return null;
    }

    // The directory's timeline: the latest it holds WAL of, and its history files, each the provider's own, though the
    // directory never reached their timelines, as a standby that fetched a timeline's history it couldn't follow has.
    string walDir = Path.Join(dataDir, "pg_wal");
    List<string> names;
    try
    {
        names = [.. Directory.EnumerateFiles(walDir).Select(path => Path.GetFileName(path))];
    }
    catch (Exception error) when (error is IOException or UnauthorizedAccessException)
    {
        return (false, $"is in WAL the run can't read: {error.Message}");
    }

    long ownTimeline = checkpointTimeline;
    List<long> ownHistories = [];
    foreach (string name in names)
    {
        if (WalSegments.TryParse(name, segmentSize, out long tli, out _) && name.Length == 24)
        {
            ownTimeline = Math.Max(ownTimeline, tli);
        }
        else if (WalSegments.TryParseHistory(name, out long history))
        {
            ownHistories.Add(history);
        }
    }

    if (ownTimeline > providerTimeline)
    {
        return (false, $"is followed by WAL on timeline {ownTimeline}, which the provider hasn't reached, so its WAL has left the provider's");
    }

    try
    {
        foreach (long history in ownHistories)
        {
            string name = string.Create(CultureInfo.InvariantCulture, $"{history:X8}.history");
            string? theirs = providerHistories.Contains(history) ? ReadProviderFile(name) : null;
            if (theirs is null || !Convert.FromHexString(theirs).AsSpan().SequenceEqual(File.ReadAllBytes(Path.Join(walDir, name))))
            {
                return (false, $"is followed by timeline {history}, whose history isn't the provider's, so its WAL has left the provider's");
            }
        }
    }
    catch (LibpqException)
    {
        return null;
    }
    catch (Exception error) when (error is IOException or UnauthorizedAccessException)
    {
        return (false, $"is in WAL the run can't read: {error.Message}");
    }

    // Where the provider left the directory's latest timeline, as the provider's history gives it, when the provider has
    // gone on to a later one.
    ulong? leftAt = null;
    if (ownTimeline < providerTimeline)
    {
        string? history;
        try
        {
            history = ReadProviderFile(string.Create(CultureInfo.InvariantCulture, $"{providerTimeline:X8}.history"));
        }
        catch (LibpqException)
        {
            return null;
        }

        foreach (string[] fields in (history is null ? [] : Encoding.ASCII.GetString(Convert.FromHexString(history)).Split('\n'))
            .Select(line => line.Split('\t', ' '))
            .Where(fields => fields.Length >= 2 && long.TryParse(fields[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out long parent) && parent == ownTimeline))
        {
            leftAt = ParseLsn(fields[1]);
        }

        if (leftAt is null)
        {
            return (false, $"is followed by WAL on timeline {ownTimeline}, which the provider's history doesn't have, so its WAL has left the provider's");
        }
    }

    using WalSegments own = new(walDir, names, ownTimeline, segmentSize);

    // The records from the checkpoint's redo point on, as far as they're whole, which recovery replays, through the
    // checkpoint record. A record that a crash of the server that wrote it cut short, which the page after it says was
    // overwritten, is passed over, as recovery passes over it, for the record that page begins with.
    if (Stream(own.Read, checkpoint, RecordHeader + 2 + sizeof(ulong)) is not (byte[] head, _) || !IsCheckpoint(head, redo))
    {
        return (false, "isn't the checkpoint record its own WAL holds there, so its WAL has left the provider's");
    }

    ulong previous = 0;
    ulong complete = 0;
    bool endsSegment = false;
    bool throughCheckpoint = false;
    ulong next = redo;
    while (true)
    {
        if (Record(next, previous, out bool switched, out ulong end))
        {
            throughCheckpoint |= next == checkpoint;
            previous = next;
            complete = end;
            endsSegment = switched;
            next = Next(end, switched);
        }
        else if (Overwritten(next) is ulong after)
        {
            next = after;
        }
        else
        {
            break;
        }
    }

    if (!throughCheckpoint)
    {
        return (false, "isn't the checkpoint record its own WAL holds there, after whole records from its redo point, so its WAL has left the provider's");
    }

    if (complete > providerEnd)
    {
        return (false, $"is followed by WAL past the provider's, to {Lsn(complete)}, so its WAL has left the provider's");
    }

    if (leftAt is ulong left && complete > left)
    {
        return (false, $"is followed by WAL on timeline {ownTimeline} past {Lsn(left)}, where the provider left that timeline, so its WAL has left the provider's");
    }

    // A record that the directory's WAL ends inside of is held as far as its pages are: each page whose next page has
    // been written, as its header's address shows, is whole, while the last may hold only part of what it will, after
    // which a segment PostgreSQL used again holds what it held before. After a WAL switch, the next record is in the next
    // segment.
    ulong held = complete;
    ulong firstPage = endsSegment ? (complete + segmentBytes - 1) / segmentBytes * segmentBytes : ((complete - 1) / pageBytes * pageBytes) + pageBytes;
    for (ulong page = firstPage; own.Read(page, 16) is byte[] header && Read64(header, 8) == page; page += pageBytes)
    {
        held = Math.Max(held, page);
    }

    // The WAL both hold, from the latest segment back, as far as the provider has it on the directory's timeline, and,
    // where the provider left that timeline, to where it left it, as recovery that follows the provider takes no record
    // past there from the directory: a segment the provider removes meanwhile ends it.
    ulong shared = Math.Min(Math.Min(held, providerEnd), leftAt ?? ulong.MaxValue);
    ulong compared = 0;
    for (ulong segment = (shared - 1) / segmentBytes; shared > redo; segment--)
    {
        ulong start = Math.Max(segment * segmentBytes, redo);
        ulong stop = Math.Min((segment + 1) * segmentBytes, shared);
        if (own.Name(start) is not string name || !providerSegments.TryGetValue(name, out string? file))
        {
            break;
        }

        bool gone = false;
        for (ulong from = start; from < stop && !gone; from += Chunk)
        {
            int length = (int)Math.Min(stop - from, Chunk);
            string?[] theirs;
            try
            {
                theirs = conn.Query(string.Create(CultureInfo.InvariantCulture, $"""
                    SELECT pg_catalog.length(b), pg_catalog.encode(pg_catalog.sha256(b), 'hex')
                    FROM (SELECT pg_catalog.pg_read_binary_file('pg_wal/{file}', {from % segmentBytes}, {length}, true)) r (b)
                    """))[0];
            }
            catch (LibpqException)
            {
                return null;
            }

            if (theirs[0] != length.ToString(CultureInfo.InvariantCulture))
            {
                gone = true;
            }
            else if (own.Read(from, length) is not byte[] ours ||
                !Convert.ToHexStringLower(SHA256.HashData(ours)).Equals(theirs[1], StringComparison.Ordinal))
            {
                return (false, $"is followed by WAL that isn't the provider's, in segment {name}, so its WAL has left the provider's");
            }
            else
            {
                compared += (ulong)length;
            }
        }

        if (gone || segment == 0 || segment * segmentBytes <= redo)
        {
            break;
        }
    }

    return compared > 0
        ? (true, null)
        : (false, $"is in WAL the provider has removed, and its own WAL, which ends at {Lsn(held)}, doesn't reach the WAL the provider still has, so nothing shows it's the provider's; keep the provider's WAL from a standby's position with a physical replication slot, as pg_basebackup -C -S makes, or wal_keep_size");

    // The provider's copy of a file in its pg_wal, as hex, or null when it has none.
    string? ReadProviderFile(string name)
        => QueryValue(conn, $"SELECT pg_catalog.encode(pg_catalog.pg_read_binary_file('pg_wal/{name}', 0, 1048576, true), 'hex')");

    // Where the record after one that ends at a position starts: the next segment's first record after a WAL switch,
    // or the next eight-byte boundary, past a page's header.
    ulong Next(ulong end, bool afterSwitch)
    {
        ulong at = afterSwitch ? ((end + segmentBytes - 1) / segmentBytes * segmentBytes) : (end + 7) & ~7UL;
        return at % pageBytes == 0 ? at + (at % segmentBytes == 0 ? (ulong)LongPageHeader : ShortPageHeader) : at;
    }

    // Where the record after one that a crash cut short starts: the first of a page within the record's length whose
    // header says the record that continued onto it was overwritten, past the header; or null when none does.
    ulong? Overwritten(ulong at)
    {
        if (Stream(own.Read, at, RecordHeader) is not (byte[] header, _))
        {
            return null;
        }

        ulong last = at + Read32(header, 0) + ((Read32(header, 0) / (pageBytes - LongPageHeader)) + 2) * LongPageHeader;
        for (ulong page = ((at / pageBytes) + 1) * pageBytes; page <= last; page += pageBytes)
        {
            if (own.Read(page, 16) is not byte[] pageHeader || Read64(pageHeader, 8) != page)
            {
                return null;
            }

            // XLP_FIRST_IS_OVERWRITE_CONTRECORD, which PostgreSQL 19, whose pages' magic is 0xD122, made 0x0004 as it
            // dropped XLP_BKP_REMOVABLE.
            if ((BitConverter.ToUInt16(pageHeader, 2) & (BitConverter.ToUInt16(pageHeader, 0) >= 0xD122 ? 0x0004 : 0x0008)) != 0)
            {
                return page + (page % segmentBytes == 0 ? (ulong)LongPageHeader : ShortPageHeader);
            }
        }

        return null;
    }

    // Reads the directory's record at a position, which must link back to the one given, if one is, and be whole, as
    // its checksum over its data, then its header to the checksum, shows; with whether it's a WAL switch, which ends its
    // segment, and where it ends.
    bool Record(ulong at, ulong previousAt, out bool isSwitch, out ulong end)
    {
        isSwitch = false;
        end = 0;
        if (Stream(own.Read, at, RecordHeader) is not (byte[] header, _))
        {
            return false;
        }

        uint total = Read32(header, 0);
        isSwitch = header[17] == 0 && (header[16] & 0xF0) == 0x40;
        if (total < RecordHeader || total > 1024 * 1024 * 1024 || (previousAt != 0 && Read64(header, 8) != previousAt) ||
            Stream(own.Read, at, (int)total) is not (byte[] record, ulong recordEnd))
        {
            return false;
        }

        end = recordEnd;
        uint crc = Crc32C(0xFFFFFFFF, record.AsSpan(RecordHeader));
        crc = Crc32C(crc, record.AsSpan(0, 20));
        return (crc ^ 0xFFFFFFFF) == Read32(record, 20);
    }

    // A number of WAL bytes from a position, past the page headers, each of which must give its page's own address, and
    // where they end.
    (byte[] Bytes, ulong End)? Stream(Func<ulong, int, byte[]?> read, ulong at, int length)
    {
        List<byte> bytes = new(length);
        while (bytes.Count < length)
        {
            if (at % pageBytes == 0)
            {
                if (read(at, 16) is not byte[] pageHeader || Read64(pageHeader, 8) != at)
                {
                    return null;
                }

                at += at % segmentBytes == 0 ? LongPageHeader : (ulong)ShortPageHeader;
                continue;
            }

            int take = (int)Math.Min((ulong)(length - bytes.Count), (((at / pageBytes) + 1) * pageBytes) - at);
            if (read(at, take) is not byte[] chunk)
            {
                return null;
            }

            bytes.AddRange(chunk);
            at += (ulong)take;
        }

        return ([.. bytes], at);
    }

    static string Lsn(ulong lsn) => string.Create(CultureInfo.InvariantCulture, $"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}");

    static ulong? ParseLsn(string? text)
        => text?.Trim().Split('/') is [string high, string low] && uint.TryParse(high, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out uint hi) &&
            uint.TryParse(low, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out uint lo)
            ? ((ulong)hi << 32) | lo : null;

    static uint Crc32C(uint crc, ReadOnlySpan<byte> data)
    {
        while (data.Length >= 8)
        {
            crc = BitOperations.Crc32C(crc, BinaryPrimitives.ReadUInt64LittleEndian(data));
            data = data[8..];
        }

        foreach (byte b in data)
        {
            crc = BitOperations.Crc32C(crc, b);
        }

        return crc;
    }

    static ulong Read64(byte[] bytes, int at)
        => BitConverter.IsLittleEndian ? BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(at)) : BinaryPrimitives.ReadUInt64BigEndian(bytes.AsSpan(at));

    static uint Read32(byte[] bytes, int at)
        => BitConverter.IsLittleEndian ? BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(at)) : BinaryPrimitives.ReadUInt32BigEndian(bytes.AsSpan(at));
}

// Whether a WAL record's start is a checkpoint's with a redo point: RM_XLOG_ID, XLOG_CHECKPOINT_SHUTDOWN or
// XLOG_CHECKPOINT_ONLINE in the info's high bits, and data whose short header comes first, which starts with the redo
// point, in the server's byte order.
static bool IsCheckpoint(byte[] record, ulong redo)
{
    const int RecordHeader = 24;
    const byte DataShort = 255;
    ulong recordRedo = BitConverter.IsLittleEndian
        ? BinaryPrimitives.ReadUInt64LittleEndian(record.AsSpan(RecordHeader + 2))
        : BinaryPrimitives.ReadUInt64BigEndian(record.AsSpan(RecordHeader + 2));
    return record[17] == 0 && (record[16] & 0xF0) is 0x00 or 0x10 && record[RecordHeader] == DataShort && recordRedo == redo;
}

// Keeps postgresql.auto.conf as it was before this run changes it, and whether the data directory had a standby.signal.
void SaveRecoverySettings()
{
    string filename = Path.Join(dataDir, "postgresql.auto.conf");
    try
    {
        autoConfBefore ??= File.Exists(filename) ? File.ReadAllBytes(filename) : [];
        hadStandbySignal = File.Exists(Path.Join(dataDir, "standby.signal"));
    }
    catch (Exception error) when (error is IOException or UnauthorizedAccessException)
    {
        throw Die($"{progname}: could not read file \"{filename}\": {error.Message}\n");
    }
}

// Write contents of postgresql.auto.conf.
void WriteRecoveryConf(string contents)
{
    string filename = Path.Join(dataDir, "postgresql.auto.conf");
    try
    {
        EndLine(filename);
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

// Find the pgport and try a connection, to the server that logs to the given file.
void WaitPostmasterConnection(string connstr, string log)
{
    long pmpid;

    PrintMsg(VerbosityLevel.Verbose, "Waiting for PostgreSQL to accept connections ...");

    // First wait for Postmaster to come up, which a server that stopped as it started, as one whose recovery can't
    // reach its minimum recovery point does, never does.
    Stopwatch coming = Stopwatch.StartNew();
    while (true)
    {
        if ((pmpid = GetPgpid()) != 0 && PostmasterIsAlive(pmpid))
        {
            break;
        }

        if (coming.Elapsed > TimeSpan.FromSeconds(30))
        {
            throw Die($"PostgreSQL in data directory \"{dataDir}\" stopped, or never came up. See {log}.\n");
        }

        Pause(TimeSpan.FromSeconds(1));
        PrintMsg(VerbosityLevel.Verbose, ".");
    }

    // Now wait for Postmaster to either accept connections or die.
    while (true)
    {
        PingStatus res = Interruptible(() => Libpq.Ping(connstr));
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
        Pause(TimeSpan.FromSeconds(1));
        PrintMsg(VerbosityLevel.Verbose, ".");
    }

    PrintMsg(VerbosityLevel.Verbose, "\n");
}

// Wait for PostgreSQL, which logs to the given file, to leave recovery/standby mode.
void WaitPrimaryConnection(string connstr, string log)
{
    bool ispri = false;
    LibpqConnection? conn = null;

    WaitPostmasterConnection(connstr, log);

    PrintMsg(VerbosityLevel.Verbose, "Waiting for PostgreSQL to become primary...");

    while (!ispri)
    {
        if (conn is null || !conn.IsOk)
        {
            conn?.Dispose();
            WaitPostmasterConnection(connstr, log);
            conn = Connectdb(connstr);
        }

        try
        {
            if (QueryValue(conn, "SELECT pg_is_in_recovery()") == "f")
            {
                ispri = true;
            }
        }
        catch (LibpqException)
        {
            // The connection broke; reconnect.
        }

        if (!ispri)
        {
            Pause(TimeSpan.FromSeconds(1));
            PrintMsg(VerbosityLevel.Verbose, ".");
        }
    }

    conn!.Dispose();
    promoted = true;
    PrintMsg(VerbosityLevel.Verbose, "\n");
}

// Wait for postmaster to die.
void WaitPostmasterShutdown()
{
    PrintMsg(VerbosityLevel.Verbose, "Waiting for PostgreSQL to shutdown ...");

    while (GetPgpid() != 0)
    {
        Pause(TimeSpan.FromSeconds(1));
        PrintMsg(VerbosityLevel.Normal, ".");
    }

    PrintMsg(VerbosityLevel.Verbose, "\n");
}

bool FileExists(string path) => File.Exists(path) || Directory.Exists(path);

bool IsPgDir(string path)
    => Directory.Exists(path) && File.Exists(Path.Join(dataDir, "PG_VERSION"));

// Ends a file's last line, if it has one without its newline, so that what's appended starts a line of its own rather
// than joining it, as a comment.
static void EndLine(string filename)
{
    using FileStream file = new(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite);
    if (file.Length > 0)
    {
        _ = file.Seek(-1, SeekOrigin.End);
        if (file.ReadByte() != '\n')
        {
            file.WriteByte((byte)'\n');
        }
    }
}

// copy one file.
void CopyFile(string fromfile, string tofile, bool append)
{
    if (append)
    {
        try
        {
            EndLine(tofile);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            throw Die($"could not write to file \"{tofile}\": {error.Message}");
        }
    }

    using FileStream srcfd = OpenFile(fromfile, FileMode.Open, FileAccess.Read, $"could not open file \"{fromfile}\"");
    using FileStream dstfd = OpenFile(tofile, append ? FileMode.Append : FileMode.Create, FileAccess.Write,
        $"could not create file \"{tofile}\"");
    try
    {
        srcfd.CopyTo(dstfd);
    }
    catch (IOException)
    {
        throw Die($"could not write to file \"{tofile}\"");
    }
}

FileStream OpenFile(string path, FileMode mode, FileAccess access, string message)
{
    try
    {
        return new FileStream(path, mode, access);
    }
    catch (Exception error) when (error is IOException or UnauthorizedAccessException)
    {
        throw Die(message);
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

// Runs a program to its end, failing the run for a stop that comes meanwhile: a program a stop ends, as pg_basebackup,
// is ended with the processes it started, and any other runs to its end first, as pg_ctl starting or stopping the
// server does, so that a server it started is stopped rather than left half started.
int RunProgram(string execPath, IEnumerable<string> arguments, bool endedByStop, bool relayed = false)
{
    CheckStop();
    (Process started, Task output) = StartProgram(execPath, arguments, relayed);
    using Process process = started;
    while (!process.WaitForExit(200))
    {
        if (endedByStop && stopped.IsSet && !committed)
        {
            try
            {
                process.Kill(true);
            }
            catch (InvalidOperationException)
            {
                // It ended meanwhile.
            }

            process.WaitForExit();
            CheckStop();
        }
    }

    // A relayed program's output is written to its end, which on Unix comes as the program, and what it left running
    // with its output, end: pg_ctl's server writes to its log. On Windows the server pg_ctl starts holds pg_ctl's output
    // open, so what comes within a moment of the exit is written.
    _ = output.Wait(OperatingSystem.IsWindows() ? TimeSpan.FromMilliseconds(500) : TimeSpan.FromSeconds(10));
    CheckStop();
    return process.ExitCode;
}

// Makes a call that may wait on a server in libpq, as connecting or a command does, on another thread, so that a stop
// before a subscription has committed fails the run at once, however long the server takes to answer, as one that
// takes the connection and answers nothing does: the call is left to end as the process exits.
T Interruptible<T>(Func<T> call)
{
    Task<T> task = Task.Run(call);
    if (WaitHandle.WaitAny([((IAsyncResult)task).AsyncWaitHandle, stopped.WaitHandle]) == 1)
    {
        CheckStop();
        task.Wait();
    }

    return task.GetAwaiter().GetResult();
}

// Runs a program to its end, whatever stops come, as the cleanup does.
int RunProgramToEnd(string execPath, IEnumerable<string> arguments)
{
    using Process process = StartProgram(execPath, arguments).Process;
    process.WaitForExit();
    return process.ExitCode;
}

// Starts a program, which shares the run's input and output, or, relayed, as pg_ctl is, has its own, whose output the
// run writes as it comes, and on Windows inherits none of the run's: there pg_ctl starts the server through cmd.exe,
// which takes pg_ctl's handles, and every other handle it may inherit, and stays the server's parent, so that a server
// started with the run's would keep the run's output open after the run ends, and a caller that reads it to its end, as
// a shell pipe does, would wait for the server to stop. The task returned ends with a relayed program's output, as
// its writers close it, at once for one that isn't relayed.
(Process Process, Task Output) StartProgram(string execPath, IEnumerable<string> arguments, bool relayed = false)
{
    ProcessStartInfo start = new(execPath)
    {
        UseShellExecute = false,
        RedirectStandardInput = relayed,
        RedirectStandardOutput = relayed,
        RedirectStandardError = relayed,
    };
    foreach (string argument in arguments)
    {
        start.ArgumentList.Add(argument);
    }

    Process process;
    if (relayed && OperatingSystem.IsWindows())
    {
        using InheritedHandles withheld = new();
        process = Process.Start(start)!;
    }
    else
    {
        process = Process.Start(start)!;
    }

    if (!relayed)
    {
        return (process, Task.CompletedTask);
    }

    TaskCompletionSource outputEnded = new(TaskCreationOptions.RunContinuationsAsynchronously);
    TaskCompletionSource errorsEnded = new(TaskCreationOptions.RunContinuationsAsynchronously);
    process.StandardInput.Close();
    process.OutputDataReceived += (_, line) =>
    {
        if (line.Data is null)
        {
            outputEnded.TrySetResult();
        }
        else
        {
            Console.Out.WriteLine(line.Data);
        }
    };
    process.ErrorDataReceived += (_, line) =>
    {
        if (line.Data is null)
        {
            errorsEnded.TrySetResult();
        }
        else
        {
            Console.Error.WriteLine(line.Data);
        }
    };
    process.BeginOutputReadLine();
    process.BeginErrorReadLine();
    return (process, Task.WhenAll(outputEnded.Task, errorsEnded.Task));
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

List<string?[]> Query(LibpqConnection conn, string sql)
{
    CheckStop();
    running = conn;
    try
    {
        return Interruptible(() => conn.Query(sql));
    }
    finally
    {
        running = null;
    }
}

string? QueryValue(LibpqConnection conn, string sql)
{
    List<string?[]> rows = Query(conn, sql);
    return rows.Count > 0 ? rows[0][0] : null;
}
