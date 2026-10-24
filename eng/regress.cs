// Runs the ported pglogical regression suite with ankus regress on a fresh development cluster, using the settings
// from pglogical's regress-postgresql.conf. --huge-tx adds the three huge_tx tests, which pglogical's default run leaves
// out, after node_origin_cascade. Usage: dotnet run eng/regress.cs -- --pg <major> [--repeat <count>] [--huge-tx] [filter]

using System.Diagnostics;
using System.Globalization;

string? major = null;
string? filter = null;
int repeat = 1;
bool hugeTx = false;
for (int index = 0; index < args.Length; index++)
{
    switch (args[index])
    {
        case "--pg":
            major = args[++index];
            break;
        case "--repeat":
            repeat = int.Parse(args[++index], CultureInfo.InvariantCulture);
            break;
        case "--huge-tx":
            hugeTx = true;
            break;
        default:
            filter = args[index];
            break;
    }
}

if (major is null)
{
    Console.Error.WriteLine("usage: dotnet run eng/regress.cs -- --pg <major> [--repeat <count>] [--huge-tx] [filter]");
    return 2;
}

string root = Path.GetFullPath(Path.Join(AppContext.GetData("EntryPointFileDirectoryPath") as string ?? ".", ".."));
string userHome = Environment.GetEnvironmentVariable("ANKUS_HOME")
    ?? Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".ankus");
string home = Path.Join(root, "artifacts", "regress-home");

// Every run starts from a fresh cluster, as pg_regress's temporary instance does, while sharing the registered
// PostgreSQL installations.
Run("dotnet", ["ankus", "stop", "--pg", major, "--home", home], check: false);
if (Directory.Exists(home))
{
    Directory.Delete(home, recursive: true);
}

Directory.CreateDirectory(home);
File.Copy(Path.Join(userHome, "config.json"), Path.Join(home, "config.json"));

List<string> settings =
[
    // Configuration that affects behaviour being tested:
    "shared_preload_libraries=worf",
    "wal_level=logical",
    "max_wal_senders=20",
    "max_replication_slots=20",
    "max_worker_processes=20",
    "track_commit_timestamp=on",

    // Purely testing related:
    "hba_file=" + Path.Join(root, "src", "Worf", "pg_regress", "regress-pg_hba.conf"),
    "DateStyle=ISO, DMY",
    // pg_regress keeps PostgreSQL's default, which ankus regress otherwise replaces with warning.
    "client_min_messages=notice",
    "log_line_prefix=[%m] [%p] [%d] ",
    "fsync=off",

    "worf.synchronous_commit=true",
    "worf.conflict_history_enabled=on",

    // Indirection of dsns for testing
    "worf.orig_provider_dsn=dbname=sourcedb",
    "worf.provider_dsn=dbname=regression",
    "worf.provider1_dsn=dbname=regression1",
    "worf.subscriber_dsn=dbname=postgres",
];

// huge_tx_100k_tables creates 100,000 tables in one transaction; its header asks for this setting.
if (hugeTx)
{
    settings.Add("max_locks_per_transaction=10000");
}

// Current PostgreSQL releases load only the output plugins this setting lists; PostgreSQL 13 has no such setting.
if (int.Parse(major, CultureInfo.InvariantCulture) >= 14)
{
    settings.Add("output_plugin_libraries=pgoutput, test_decoding, worf");
}

List<string> arguments =
[
    "ankus", "regress", "--pg", major, "--home", home, "--project", Path.Join(root, "src", "Worf", "Worf.csproj"),
    "--database", "regression", "--psql-verbosity", "default", "--repeat", repeat.ToString(CultureInfo.InvariantCulture),
];
foreach (string setting in settings)
{
    arguments.Add("--postgresql-conf");
    arguments.Add(setting);
}

if (filter is not null)
{
    arguments.Add(filter);
}

// The huge_tx tests run after node_origin_cascade and before drop, which removes the nodes they use.
string suite = Path.Join(root, "src", "Worf", "pg_regress");
List<string> added = [];
if (hugeTx)
{
    foreach (string directory in (string[])["sql", "expected"])
    {
        foreach (string file in Directory.GetFiles(Path.Join(suite, "huge_tx", directory)))
        {
            string copy = Path.Join(suite, directory, "29a_" + Path.GetFileName(file));
            File.Copy(file, copy, overwrite: true);
            added.Add(copy);
        }
    }
}

int code;
try
{
    code = Run("dotnet", arguments, check: false);
}
finally
{
    foreach (string file in added)
    {
        File.Delete(file);
    }
}

Run("dotnet", ["ankus", "stop", "--pg", major, "--home", home], check: false);

string diffs = Path.Join(root, "src", "Worf", "pg_regress", "regression.diffs");
if (code != 0 && File.Exists(diffs))
{
    Console.WriteLine(File.ReadAllText(diffs));
}

string log = Path.Join(home, "clusters", $"pg{major}.log");
if (code != 0 && File.Exists(log))
{
    Console.WriteLine($"Server log {log}:");
    Console.WriteLine(File.ReadAllText(log));
}

return code;

static int Run(string fileName, IEnumerable<string> arguments, bool check = true)
{
    ProcessStartInfo start = new(fileName) { UseShellExecute = false };
    foreach (string argument in arguments)
    {
        start.ArgumentList.Add(argument);
    }

    Console.WriteLine("> " + fileName + " " + string.Join(' ', start.ArgumentList.Select(static argument => '"' + argument + '"')));
    using Process process = Process.Start(start)!;
    process.WaitForExit();
    if (check && process.ExitCode != 0)
    {
        throw new InvalidOperationException($"{fileName} exited with code {process.ExitCode}.");
    }

    return process.ExitCode;
}
